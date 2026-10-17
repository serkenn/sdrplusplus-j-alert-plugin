#include "sink/file_sink.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>

#include <sys/stat.h>
#ifdef _WIN32
#include <io.h>
#endif

namespace jalert::sink {

const char* to_string(SinkPhase p) {
    switch (p) {
        case SinkPhase::Active: return "Active";
        case SinkPhase::Error:  return "Error";
        case SinkPhase::Off:
        default:                return "Off";
    }
}

namespace {
// Size of the file (0 if it does not exist) and the offset just past its last
// '\n' (0 if none), i.e. the end of the last complete record.
bool scan_tail(const std::string& path, std::uintmax_t& size,
               std::uintmax_t& record_end, std::string& err) {
    size = record_end = 0;
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        if (ec) { err = ec.message(); return false; }
        return true;
    }
    size = std::filesystem::file_size(path, ec);
    if (ec) { err = ec.message(); return false; }

    std::ifstream in(path, std::ios::binary);
    if (!in) { err = "cannot read file"; return false; }
    char buf[4096];
    std::uintmax_t pos = size;
    while (pos > 0) {
        const std::uintmax_t n = std::min<std::uintmax_t>(pos, sizeof(buf));
        pos -= n;
        in.seekg(static_cast<std::streamoff>(pos));
        if (!in.read(buf, static_cast<std::streamsize>(n))) {
            err = "cannot read file";
            return false;
        }
        for (std::uintmax_t i = n; i > 0; --i) {
            if (buf[i - 1] == '\n') { record_end = pos + i; return true; }
        }
    }
    return true;
}

// Current size of the open file, i.e. where the next append lands.
bool open_file_size(std::FILE* fp, std::uintmax_t& size) {
#ifdef _WIN32
    struct _stat64 st;
    if (_fstat64(_fileno(fp), &st) != 0) return false;
#else
    struct stat st;
    if (fstat(fileno(fp), &st) != 0) return false;
#endif
    size = static_cast<std::uintmax_t>(st.st_size);
    return true;
}

std::string path_key(const std::string& path) {
    std::error_code ec;
    const auto abs = std::filesystem::absolute(path, ec);
    if (ec) return path;
    const auto canon = std::filesystem::weakly_canonical(abs, ec);
    return ec ? abs.string() : canon.string();
}
} // namespace

struct FileJsonlSink::SharedFile {
    std::mutex mu;   // serializes appends and rollback on this file
    bool partial = false;   // a failed write may have left bytes past trim_to
    std::uintmax_t trim_to = 0;   // file size just before that write
};

// Every sink on the same path (e.g. several module instances on the default
// output file) gets the same SharedFile.
std::shared_ptr<FileJsonlSink::SharedFile>
FileJsonlSink::shared_file(const std::string& path) {
    static std::mutex registry_mu;
    static std::map<std::string, std::weak_ptr<SharedFile>> registry;
    std::lock_guard<std::mutex> lk(registry_mu);
    for (auto it = registry.begin(); it != registry.end();) {
        it = it->second.expired() ? registry.erase(it) : std::next(it);
    }
    auto& slot = registry[path_key(path)];
    auto f = slot.lock();
    if (!f) {
        f = std::make_shared<SharedFile>();
        slot = f;
    }
    return f;
}

FileJsonlSink::FileJsonlSink(const std::string& path) : path_(path) {
    try {
        std::filesystem::path p(path);
        if (p.has_parent_path() && !p.parent_path().empty()) {
            std::error_code ec;
            std::filesystem::create_directories(p.parent_path(), ec);
        }
    } catch (...) {
        // best-effort directory create — fall through to fopen
    }

    file_ = shared_file(path);
    std::lock_guard<std::mutex> lk(gate_);
    std::lock_guard<std::mutex> flk(file_->mu);
    open_locked();
}

// Opens the file positioned at a complete-record boundary. On failure the sink
// stays in the error state and the next write_line retries.
bool FileJsonlSink::open_locked() {
    std::string err;
    if (file_->partial && !trim_partial_locked(err)) {
        fail_locked("cannot trim incomplete record: " + err);
        return false;
    }

    std::uintmax_t size = 0, record_end = 0;
    if (!scan_tail(path_, size, record_end, err)) { fail_locked(err); return false; }
    // Existing file not ending in '\n' (not ours, or left by a crash): start a
    // new line instead of extending its last one.
    const bool need_newline = record_end < size;

    fp_ = std::fopen(path_.c_str(), "ab");
    if (!fp_) { fail_locked(std::strerror(errno)); return false; }
    std::setvbuf(fp_, nullptr, _IOFBF, 1 << 16);
    if (need_newline && (std::fputc('\n', fp_) == EOF || std::fflush(fp_) != 0)) {
        fail_locked(std::strerror(errno));
        close_locked();
        return false;
    }
    status_ = std::string("writing → ") +
              std::filesystem::path(path_).filename().string();
    error_.clear();
    return true;
}

// Cuts the file back to its size before the failed write, dropping whatever
// that write left behind (even if it ends in '\n'). Sinks on this file append
// only under file_->mu and not while a trim is pending, so nothing past that
// offset is another sink's record. If the file has shrunk below the offset
// meanwhile (rotated or truncated externally), falls back to its last line
// break.
bool FileJsonlSink::trim_partial_locked(std::string& err) {
    std::uintmax_t size = 0, record_end = 0;
    if (!scan_tail(path_, size, record_end, err)) return false;
    const std::uintmax_t keep =
        size >= file_->trim_to ? file_->trim_to : record_end;
    if (size > keep) {
        std::error_code ec;
        std::filesystem::resize_file(path_, keep, ec);
        if (ec) { err = ec.message(); return false; }
    }
    file_->partial = false;
    return true;
}

void FileJsonlSink::close_locked() {
    if (fp_) {
        std::fclose(fp_);
        fp_ = nullptr;
    }
}

void FileJsonlSink::fail_locked(const std::string& msg) {
    error_ = msg;
    status_ = "error: " + error_;
}

FileJsonlSink::~FileJsonlSink() { stop(); }

std::string FileJsonlSink::status() const {
    std::lock_guard<std::mutex> lk(gate_);
    return status_;
}

void FileJsonlSink::write_line(const std::string& line) {
    if (line.empty()) return;
    std::lock_guard<std::mutex> lk(gate_);
    if (stopped_) return;
    std::lock_guard<std::mutex> flk(file_->mu);
    // Another sink on this file left a fragment it could not trim yet: reopen,
    // which trims first, instead of appending after it.
    if (file_->partial) close_locked();
    // Retry after an earlier open/write failure (e.g. a drive that came back).
    if (!fp_ && !open_locked()) return;

    // Rollback point: the actual end of the file, which includes records
    // appended by other sinks (each line is flushed, so nothing is buffered).
    std::uintmax_t before = 0;
    if (!open_file_size(fp_, before)) {
        fail_locked(std::strerror(errno));
        close_locked();
        return;
    }

    // Record and newline in one write: after a short write nothing more is
    // appended, so a fragment never gets a line break of its own.
    const std::string record = line + '\n';
    const size_t n = std::fwrite(record.data(), 1, record.size(), fp_);
    if (n != record.size() || std::fflush(fp_) != 0) {
        fail_locked(std::strerror(errno));
        close_locked();
        // Part of the record may have reached the file; drop it now if
        // possible, otherwise before the next append by any sink on this file.
        file_->partial = true;
        file_->trim_to = before;
        std::string ignored;
        trim_partial_locked(ignored);
        return;
    }
    bytes_written_ += static_cast<long long>(record.size());
    ++records_written_;
}

void FileJsonlSink::stop() {
    std::lock_guard<std::mutex> lk(gate_);
    stopped_ = true;
    if (fp_) std::fflush(fp_);
    close_locked();
    status_ = "off";
}

FileSinkSnapshot FileJsonlSink::snapshot() const {
    std::lock_guard<std::mutex> lk(gate_);
    FileSinkSnapshot s;
    s.path = path_;
    s.bytes_written = bytes_written_;
    s.records_written = records_written_;
    s.error_message = error_;
    if (!error_.empty())   s.phase = SinkPhase::Error;
    else if (fp_)          s.phase = SinkPhase::Active;
    else                   s.phase = SinkPhase::Off;
    return s;
}

} // namespace jalert::sink

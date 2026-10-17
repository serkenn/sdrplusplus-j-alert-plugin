#include "sink/file_sink.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <vector>

#include <sys/stat.h>
#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
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

#ifdef _WIN32
using FileStat = struct ::_stat64;
bool stat_open_file(std::FILE* fp, FileStat& st) {
    return _fstat64(_fileno(fp), &st) == 0;
}
#else
using FileStat = struct ::stat;
bool stat_open_file(std::FILE* fp, FileStat& st) {
    return fstat(fileno(fp), &st) == 0;
}
#endif

// Current size of the open file, i.e. where the next append lands.
bool open_file_size(std::FILE* fp, std::uintmax_t& size) {
    FileStat st;
    if (!stat_open_file(fp, st)) return false;
    size = static_cast<std::uintmax_t>(st.st_size);
    return true;
}

// Whether two handles refer to the same file. On Windows the CRT opens files
// without FILE_SHARE_DELETE, so an open file cannot be renamed or replaced and
// two handles on one path are always the same file.
bool same_file(std::FILE* a, std::FILE* b) {
#ifdef _WIN32
    (void)a; (void)b;
    return true;
#else
    FileStat sa, sb;
    if (!stat_open_file(a, sa) || !stat_open_file(b, sb)) return true;
    return sa.st_dev == sb.st_dev && sa.st_ino == sb.st_ino;
#endif
}

// Cuts the file behind `fp` back to `keep` bytes. Works on the handle, not the
// path, so a file renamed and replaced meanwhile is never touched. A file that
// is already no longer than `keep` was truncated externally; leave it.
bool truncate_open_file(std::FILE* fp, std::uintmax_t keep, std::string& err) {
    std::uintmax_t size = 0;
    if (!open_file_size(fp, size)) { err = std::strerror(errno); return false; }
    if (size <= keep) return true;
#ifdef _WIN32
    const int e = _chsize_s(_fileno(fp), static_cast<__int64>(keep));
    if (e != 0) { err = std::strerror(e); return false; }
#else
    if (ftruncate(fileno(fp), static_cast<off_t>(keep)) != 0) {
        err = std::strerror(errno);
        return false;
    }
#endif
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
    // A failed write whose fragment could not be cut off yet. The handle is
    // kept so the retry trims the same file, whatever `path` names by then.
    struct Pending {
        std::FILE* fp;
        std::uintmax_t trim_to;   // file size just before that write
    };

    std::mutex mu;   // serializes appends and rollback on this file
    std::vector<Pending> pending;

    ~SharedFile() {
        for (auto& p : pending) std::fclose(p.fp);
    }
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
    fp_ = std::fopen(path_.c_str(), "ab");
    if (!fp_) { fail_locked(std::strerror(errno)); return false; }
    // Unbuffered: whatever fwrite() did not write is never flushed later (e.g.
    // by fclose), so after a rollback nothing more lands in the file.
    std::setvbuf(fp_, nullptr, _IONBF, 0);

    // A fragment of ours still in this file must go before the tail check
    // below, or it would be kept as a line of its own.
    std::string err;
    if (!settle_pending_locked(err)) {
        fail_locked("cannot trim incomplete record: " + err);
        close_locked();
        return false;
    }

    std::uintmax_t size = 0, record_end = 0;
    if (!scan_tail(path_, size, record_end, err)) {
        fail_locked(err);
        close_locked();
        return false;
    }
    // Existing file not ending in '\n' (not ours, or left by a crash): start a
    // new line instead of extending its last one.
    if (record_end < size && std::fputc('\n', fp_) == EOF) {
        fail_locked(std::strerror(errno));
        close_locked();
        return false;
    }
    status_ = std::string("writing → ") +
              std::filesystem::path(path_).filename().string();
    error_.clear();
    return true;
}

// Retries the rollback of earlier failed writes on their own handles.
// Returns false if one of them is still pending on the file fp_ refers to.
bool FileJsonlSink::settle_pending_locked(std::string& err) {
    auto& pending = file_->pending;
    bool blocked = false;
    for (auto it = pending.begin(); it != pending.end();) {
        std::string e;
        if (truncate_open_file(it->fp, it->trim_to, e)) {
            std::fclose(it->fp);
            it = pending.erase(it);
            continue;
        }
        if (fp_ && same_file(it->fp, fp_)) {
            blocked = true;
            err = e;
        }
        ++it;
    }
    return !blocked;
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
    // Retry after an earlier open/write failure (e.g. a drive that came back).
    if (!fp_ && !open_locked()) return;
    // Never append after a fragment that is still waiting to be cut off.
    std::string err;
    if (!settle_pending_locked(err)) {
        fail_locked("cannot trim incomplete record: " + err);
        close_locked();
        return;
    }

    // Rollback point: the actual end of the file, which includes records
    // appended by other sinks (the stream is unbuffered, so this is exact).
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
    if (n != record.size()) {
        fail_locked(std::strerror(errno));
        // Part of the record may have reached the file: cut it off through
        // this same handle, which still refers to the file written even if
        // the path was renamed and recreated. If that fails, keep the handle
        // and retry before the next append to that file.
        if (truncate_open_file(fp_, before, err)) {
            close_locked();
        } else {
            file_->pending.push_back({fp_, before});
            fp_ = nullptr;
        }
        return;
    }
    bytes_written_ += static_cast<long long>(record.size());
    ++records_written_;
}

void FileJsonlSink::stop() {
    std::lock_guard<std::mutex> lk(gate_);
    stopped_ = true;
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

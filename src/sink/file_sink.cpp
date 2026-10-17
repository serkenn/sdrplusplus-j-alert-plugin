#include "sink/file_sink.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>

#include <sys/stat.h>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX   // keep the min/max macros away from std::min
#include <windows.h>

#include <io.h>
#else
#include <sys/file.h>
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

// Everything below works on the open handle, never on the path: after the
// output file is renamed and recreated, the path names a different file, and
// only the handle still refers to the one being appended to.
namespace {
int fd_of(std::FILE* fp) {
#ifdef _WIN32
    return _fileno(fp);
#else
    return fileno(fp);
#endif
}

// Exclusive lock on the whole file, held across a record's size check, write
// and rollback. flock(2) and LockFileEx() both lock per open handle rather
// than per process, so two sinks in one SDR++ contend just like two SDR++
// instances do (POSIX fcntl/lockf locks would not, and would be dropped by an
// unrelated close in the same process).
bool lock_file(std::FILE* fp, std::string& err) {
#ifdef _WIN32
    auto h = reinterpret_cast<HANDLE>(_get_osfhandle(fd_of(fp)));
    OVERLAPPED ov{};
    if (!LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK, 0, MAXDWORD, MAXDWORD, &ov)) {
        err = "cannot lock file";
        return false;
    }
#else
    while (flock(fd_of(fp), LOCK_EX) != 0) {
        if (errno == EINTR) continue;
        err = std::strerror(errno);
        return false;
    }
#endif
    return true;
}

void unlock_file(std::FILE* fp) {
#ifdef _WIN32
    auto h = reinterpret_cast<HANDLE>(_get_osfhandle(fd_of(fp)));
    OVERLAPPED ov{};
    UnlockFileEx(h, 0, MAXDWORD, MAXDWORD, &ov);
#else
    flock(fd_of(fp), LOCK_UN);
#endif
}

bool file_size_of(std::FILE* fp, std::uintmax_t& size) {
#ifdef _WIN32
    struct ::_stat64 st;
    if (_fstat64(fd_of(fp), &st) != 0) return false;
#else
    struct ::stat st;
    if (fstat(fd_of(fp), &st) != 0) return false;
#endif
    size = static_cast<std::uintmax_t>(st.st_size);
    return true;
}

// Reads n bytes at `off` without disturbing where the next append lands (the
// handle is O_APPEND, so writes always go to the end).
bool read_at(std::FILE* fp, char* buf, std::size_t n, std::uintmax_t off) {
#ifdef _WIN32
    const int fd = fd_of(fp);
    if (_lseeki64(fd, static_cast<__int64>(off), SEEK_SET) < 0) return false;
    return _read(fd, buf, static_cast<unsigned>(n)) == static_cast<int>(n);
#else
    return pread(fd_of(fp), buf, n, static_cast<off_t>(off)) ==
           static_cast<ssize_t>(n);
#endif
}

// Size of the open file and the offset just past its last '\n' (0 if none),
// i.e. the end of the last complete record.
bool scan_tail(std::FILE* fp, std::uintmax_t& size, std::uintmax_t& record_end,
               std::string& err) {
    record_end = 0;
    if (!file_size_of(fp, size)) { err = std::strerror(errno); return false; }

    char buf[4096];
    std::uintmax_t pos = size;
    while (pos > 0) {
        const std::uintmax_t n = std::min<std::uintmax_t>(pos, sizeof(buf));
        pos -= n;
        if (!read_at(fp, buf, static_cast<std::size_t>(n), pos)) {
            err = "cannot read file";
            return false;
        }
        for (std::uintmax_t i = n; i > 0; --i) {
            if (buf[i - 1] == '\n') { record_end = pos + i; return true; }
        }
    }
    return true;
}

bool truncate_open_file(std::FILE* fp, std::uintmax_t keep, std::string& err) {
#ifdef _WIN32
    const int e = _chsize_s(fd_of(fp), static_cast<__int64>(keep));
    if (e != 0) { err = std::strerror(e); return false; }
#else
    if (ftruncate(fd_of(fp), static_cast<off_t>(keep)) != 0) {
        err = std::strerror(errno);
        return false;
    }
#endif
    return true;
}
} // namespace

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

    std::lock_guard<std::mutex> lk(gate_);
    open_locked();
}

// Opens the file positioned at a complete-record boundary. On failure the sink
// stays in the error state and the next write_line retries.
bool FileJsonlSink::open_locked() {
    // Read access ("a+b", not "ab") so the tail check and the rollback can go
    // through this handle; writes still always append.
    fp_ = std::fopen(path_.c_str(), "a+b");
    if (!fp_) { fail_locked(std::strerror(errno)); return false; }
    // Unbuffered: whatever fwrite() could not write is never flushed later
    // (e.g. by fclose), so after a rollback nothing more lands in the file.
    std::setvbuf(fp_, nullptr, _IONBF, 0);

    std::string err;
    if (!lock_locked(err)) { fail_locked(err); close_locked(); return false; }
    std::uintmax_t size = 0, record_end = 0;
    const bool ok = scan_tail(fp_, size, record_end, err);
    // Existing file not ending in '\n' (not ours, or left by a crash): start a
    // new line instead of extending its last one.
    if (ok && record_end < size && std::fputc('\n', fp_) == EOF) {
        err = std::strerror(errno);
        unlock_locked();
        fail_locked(err);
        close_locked();
        return false;
    }
    unlock_locked();
    if (!ok) { fail_locked(err); close_locked(); return false; }
    ok_locked();
    return true;
}

bool FileJsonlSink::lock_locked(std::string& err) {
    if (locked_) return true;   // still held from a rollback that could not run
    if (!lock_file(fp_, err)) return false;
    locked_ = true;
    return true;
}

void FileJsonlSink::unlock_locked() {
    if (!locked_) return;
    unlock_file(fp_);
    locked_ = false;
}

// Cuts the file back to its size before the failed write, dropping whatever
// that write left behind (even if it ends in '\n'). Nothing past that offset
// is anyone else's record: appends to this file happen only under the lock.
// If the file has shrunk below the offset meanwhile (truncated externally),
// falls back to its last line break.
bool FileJsonlSink::trim_pending_locked(std::string& err) {
    std::uintmax_t size = 0, record_end = 0;
    if (!scan_tail(fp_, size, record_end, err)) return false;
    const std::uintmax_t keep = size >= trim_to_ ? trim_to_ : record_end;
    if (size > keep && !truncate_open_file(fp_, keep, err)) return false;
    pending_ = false;
    return true;
}

void FileJsonlSink::close_locked() {
    if (fp_) {
        unlock_locked();
        std::fclose(fp_);
        fp_ = nullptr;
    }
}

void FileJsonlSink::ok_locked() {
    error_.clear();
    status_ = std::string("writing → ") +
              std::filesystem::path(path_).filename().string();
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
    // Retry after an earlier open/write failure (e.g. a drive that came back).
    if (!fp_ && !open_locked()) return;

    std::string err;
    if (!lock_locked(err)) { fail_locked(err); close_locked(); return; }
    // A fragment of an earlier failed write is still in the file. The lock has
    // been held since, so nobody appended after it; cut it off before writing.
    if (pending_ && !trim_pending_locked(err)) {
        fail_locked("cannot trim incomplete record: " + err);
        return;   // keeps the lock, so no other sink appends to the fragment
    }

    // Rollback point: the end of the file as it is now, which includes records
    // other sinks appended while they held the lock.
    std::uintmax_t before = 0;
    if (!file_size_of(fp_, before)) {
        fail_locked(std::strerror(errno));
        close_locked();
        return;
    }

    // Record and newline in one write: after a short write nothing more is
    // appended, so a fragment never gets a line break of its own.
    const std::string record = line + '\n';
    const std::size_t n = std::fwrite(record.data(), 1, record.size(), fp_);
    if (n != record.size()) {
        fail_locked(std::strerror(errno));
        trim_to_ = before;
        if (trim_pending_locked(err)) {
            close_locked();   // unlocks; the next write reopens and retries
        } else {
            // Keep the handle and the lock: the fragment is still there, and
            // no one may append after it until it is gone.
            pending_ = true;
        }
        return;
    }
    unlock_locked();
    bytes_written_ += static_cast<long long>(record.size());
    ++records_written_;
    ok_locked();
}

void FileJsonlSink::stop() {
    std::lock_guard<std::mutex> lk(gate_);
    stopped_ = true;
    // Last chance to drop a fragment an earlier rollback could not remove.
    if (fp_ && pending_) {
        std::string err;
        trim_pending_locked(err);
    }
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

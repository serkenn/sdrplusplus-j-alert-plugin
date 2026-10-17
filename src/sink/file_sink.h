#pragma once
// Append-mode JSONL file sink. Each line is written straight through (the
// stream is unbuffered) before write_line returns. No fsync per line (a slow
// disk must not stall the decoder).
//
// A record's size check, write and rollback run under an exclusive OS lock on
// the file, and the rollback goes through the handle that made the write. So a
// failed write is cut back to exactly what it added, even when other sinks —
// in this process or in another SDR++ instance — append to the same file, and
// even when the path has since been renamed and recreated. Appends therefore
// always resume on a record boundary and every line in the file is a complete
// record.

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>

#include "sink/sink.h"

namespace jalert::sink {

struct FileSinkSnapshot {
    SinkPhase phase = SinkPhase::Off;
    std::string path;
    long long bytes_written = 0;
    long long records_written = 0;
    std::string error_message;
};

class FileJsonlSink : public IJsonlSink {
public:
    explicit FileJsonlSink(const std::string& path);
    ~FileJsonlSink() override;

    std::string status() const override;
    void write_line(const std::string& line) override;
    void stop() override;

    FileSinkSnapshot snapshot() const;

private:
    mutable std::mutex gate_;
    std::string path_;
    bool open_locked();                            // gate_ held
    bool lock_locked(std::string& err);            // gate_ held
    void unlock_locked();                          // gate_ held
    bool trim_pending_locked(std::string& err);    // gate_ and file lock held
    void close_locked();                           // gate_ held
    void ok_locked();                              // gate_ held
    void fail_locked(const std::string& msg);      // gate_ held
    std::FILE* fp_ = nullptr;
    bool locked_ = false;   // file lock held (kept while a rollback is due)
    bool stopped_ = false;
    bool pending_ = false;   // a failed write left a fragment to cut off
    std::uintmax_t trim_to_ = 0;   // file size just before that write
    std::string status_;
    std::string error_;
    long long bytes_written_ = 0;
    long long records_written_ = 0;
};

} // namespace jalert::sink

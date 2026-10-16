#pragma once
// Append-mode JSONL file sink. Each line is written and flushed before
// write_line returns. No fsync per line (a slow disk must not stall the
// decoder). After a failed write the file is cut back to the end of the last
// complete record and the sink retries on the next line, so appends always
// resume on a record boundary.

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
    bool open_locked();   // gate_ held
    void fail_locked(const std::string& msg);   // gate_ held
    bool trim_partial_locked(std::string& err);   // gate_ held
    std::FILE* fp_ = nullptr;
    bool stopped_ = false;
    bool partial_ = false;   // a failed write may have left an incomplete record
    std::uintmax_t good_size_ = 0;   // file size after the last complete record
    std::string status_;
    std::string error_;
    long long bytes_written_ = 0;
    long long records_written_ = 0;
};

} // namespace jalert::sink

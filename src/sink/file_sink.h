#pragma once
// Append-mode JSONL file sink. Each line is written and flushed before
// write_line returns. No fsync per line (a slow disk must not stall the
// decoder). After a failed write the file is cut back to its size just before
// that write and the sink retries on the next line, so appends always resume on
// a record boundary. Sinks in this process that target the same file share one
// lock, so one sink's rollback never removes another sink's records.

#include <cstdint>
#include <cstdio>
#include <memory>
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
    struct SharedFile;   // per-file state shared by every sink on that path
    static std::shared_ptr<SharedFile> shared_file(const std::string& path);

    mutable std::mutex gate_;
    std::string path_;
    std::shared_ptr<SharedFile> file_;
    bool open_locked();   // gate_ and file_->mu held
    void fail_locked(const std::string& msg);   // gate_ held
    bool trim_partial_locked(std::string& err);   // file_->mu held
    void close_locked();   // gate_ held
    std::FILE* fp_ = nullptr;
    bool stopped_ = false;
    std::string status_;
    std::string error_;
    long long bytes_written_ = 0;
    long long records_written_ = 0;
};

} // namespace jalert::sink

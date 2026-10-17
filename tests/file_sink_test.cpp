// Regression tests for FileJsonlSink failure recovery.
//
// The partial-write test needs RLIMIT_FSIZE to make a write fail part-way, so
// it only runs on POSIX; the other tests are portable.

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "sink/file_sink.h"

#ifndef _WIN32
#include <csignal>
#include <sys/resource.h>
#endif

using jalert::sink::FileJsonlSink;

namespace {

int failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            ++failures;                                                      \
        }                                                                    \
    } while (0)

#define CHECK_EQ(a, b)                                                       \
    do {                                                                     \
        const auto& va = (a);                                                \
        const auto& vb = (b);                                                \
        if (!(va == vb)) {                                                   \
            std::fprintf(stderr, "%s:%d: CHECK_EQ failed: %s == %s\n",       \
                         __FILE__, __LINE__, #a, #b);                        \
            ++failures;                                                      \
        }                                                                    \
    } while (0)

std::string temp_path(const char* name) {
    const auto p = std::filesystem::temp_directory_path() / name;
    std::filesystem::remove(p);
    return p.string();
}

std::string read_all(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

void write_raw(const std::string& path, const std::string& data) {
    std::ofstream(path, std::ios::binary) << data;
}

// A file that does not end in '\n' gets a line break before the first record.
void test_unterminated_existing_file() {
    const std::string path = temp_path("jalert_sink_unterminated.jsonl");
    write_raw(path, "{\"old\":1}\n{\"tor");
    {
        FileJsonlSink sink(path);
        sink.write_line("{\"a\":1}");
        CHECK(sink.snapshot().error_message.empty());
        CHECK_EQ(sink.snapshot().records_written, 1LL);
    }
    CHECK_EQ(read_all(path), std::string("{\"old\":1}\n{\"tor\n{\"a\":1}\n"));
    std::filesystem::remove(path);
}

// A properly terminated file is appended to unchanged.
void test_append_existing_file() {
    const std::string path = temp_path("jalert_sink_append.jsonl");
    write_raw(path, "{\"old\":1}\n");
    {
        FileJsonlSink sink(path);
        sink.write_line("{\"a\":1}");
    }
    CHECK_EQ(read_all(path), std::string("{\"old\":1}\n{\"a\":1}\n"));
    std::filesystem::remove(path);
}

void test_no_writes_after_stop() {
    const std::string path = temp_path("jalert_sink_stop.jsonl");
    {
        FileJsonlSink sink(path);
        sink.write_line("{\"a\":1}");
        sink.stop();
        sink.write_line("{\"b\":2}");
        CHECK_EQ(sink.snapshot().records_written, 1LL);
    }
    CHECK_EQ(read_all(path), std::string("{\"a\":1}\n"));
    std::filesystem::remove(path);
}

#ifndef _WIN32
rlim_t set_fsize_limit(rlim_t soft) {
    rlimit rl{};
    getrlimit(RLIMIT_FSIZE, &rl);
    const rlim_t old = rl.rlim_cur;
    rl.rlim_cur = soft;
    setrlimit(RLIMIT_FSIZE, &rl);
    return old;
}

// Partial write -> failure -> recovery -> next record: the incomplete record
// is trimmed, so every line in the file is a complete record.
void test_partial_write_recovery() {
    std::signal(SIGXFSZ, SIG_IGN);   // get EFBIG instead of being killed
    const std::string path = temp_path("jalert_sink_partial.jsonl");
    {
        FileJsonlSink sink(path);
        sink.write_line("{\"zero\":0}");
        CHECK(sink.snapshot().error_message.empty());

        // Room for only part of the next record.
        const rlim_t old = set_fsize_limit(std::filesystem::file_size(path) + 8);
        sink.write_line("{\"first\":1}");
        set_fsize_limit(old);
        CHECK(!sink.snapshot().error_message.empty());
        CHECK_EQ(sink.snapshot().records_written, 1LL);

        sink.write_line("{\"second\":2}");
        CHECK(sink.snapshot().error_message.empty());
        CHECK_EQ(sink.snapshot().records_written, 2LL);
    }
    CHECK_EQ(read_all(path), std::string("{\"zero\":0}\n{\"second\":2}\n"));
    std::filesystem::remove(path);
}

// Two sinks on one file (several module instances share the default output
// path): rolling back A's failed write must not remove B's record.
void test_shared_file_rollback() {
    std::signal(SIGXFSZ, SIG_IGN);
    const std::string path = temp_path("jalert_sink_shared.jsonl");
    {
        FileJsonlSink a(path);
        FileJsonlSink b(path);
        a.write_line("{\"from_a\":1}");
        b.write_line("{\"from_b\":2}");
        CHECK(a.snapshot().error_message.empty());
        CHECK(b.snapshot().error_message.empty());

        const rlim_t old = set_fsize_limit(std::filesystem::file_size(path) + 8);
        a.write_line("{\"failed\":0}");
        set_fsize_limit(old);
        CHECK(!a.snapshot().error_message.empty());
        CHECK_EQ(read_all(path),
                 std::string("{\"from_a\":1}\n{\"from_b\":2}\n"));

        a.write_line("{\"after\":3}");
        b.write_line("{\"from_b\":4}");
        CHECK(a.snapshot().error_message.empty());
        CHECK(b.snapshot().error_message.empty());
        CHECK_EQ(a.snapshot().records_written, 2LL);
        CHECK_EQ(b.snapshot().records_written, 2LL);
    }
    CHECK_EQ(read_all(path),
             std::string("{\"from_a\":1}\n{\"from_b\":2}\n{\"after\":3}\n"
                         "{\"from_b\":4}\n"));
    std::filesystem::remove(path);
}

rlim_t g_restore_limit = RLIM_INFINITY;
extern "C" void restore_limit_on_xfsz(int) {
    // Linux setrlimit is a plain syscall; fine to call from this handler.
    rlimit rl{};
    getrlimit(RLIMIT_FSIZE, &rl);
    rl.rlim_cur = g_restore_limit;
    setrlimit(RLIMIT_FSIZE, &rl);
}

// Write availability returns in the middle of a record: the body is written
// only partly, then further writes succeed again. Nothing may be appended to
// the fragment (in particular not its newline), and the fragment must be gone
// before the next record.
void test_availability_returns_mid_record() {
    const std::string path = temp_path("jalert_sink_midrecord.jsonl");
    {
        FileJsonlSink sink(path);
        sink.write_line("{\"zero\":0}");
        CHECK(sink.snapshot().error_message.empty());

        // Larger than the sink's 64 KiB stdio buffer, so fwrite() itself hits
        // the limit: 8 bytes are written, the next write raises SIGXFSZ, and the
        // handler lifts the limit again before fwrite() returns short.
        const std::string big = "{\"big\":\"" + std::string(200000, 'x') + "\"}";
        g_restore_limit = set_fsize_limit(std::filesystem::file_size(path) + 8);
        std::signal(SIGXFSZ, restore_limit_on_xfsz);
        sink.write_line(big);
        std::signal(SIGXFSZ, SIG_IGN);
        set_fsize_limit(g_restore_limit);
        CHECK(!sink.snapshot().error_message.empty());
        CHECK_EQ(sink.snapshot().records_written, 1LL);
        // The fragment is dropped right away, not only on recovery.
        CHECK_EQ(read_all(path), std::string("{\"zero\":0}\n"));

        sink.write_line("{\"second\":2}");
        CHECK(sink.snapshot().error_message.empty());
        CHECK_EQ(sink.snapshot().records_written, 2LL);
    }
    CHECK_EQ(read_all(path), std::string("{\"zero\":0}\n{\"second\":2}\n"));
    std::filesystem::remove(path);
}
#endif

} // namespace

int main() {
    test_unterminated_existing_file();
    test_append_existing_file();
    test_no_writes_after_stop();
#ifndef _WIN32
    test_partial_write_recovery();
    test_availability_returns_mid_record();
    test_shared_file_rollback();
#endif
    if (failures) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    std::puts("all file sink tests passed");
    return 0;
}

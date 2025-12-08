#include "uring/core/connection.hpp"
#include "uring/core/engine.hpp"

struct automata_t {

    engine_t& engine;
    scratch_space_t& scratch;
    connection_t& connection;
    stage_t completed_stage{};
    int completed_result{};

    void operator()() noexcept;
    bool is_corrupted() const noexcept { return completed_result == -EPIPE || completed_result == -EBADF; }

    // Computed properties:
    bool received_full_request() const noexcept;
    bool should_release() const noexcept;

    // Submitting to io_uring:
    void send_next() noexcept;
    void receive_next() noexcept;
    void close_gracefully() noexcept;

    // Helpers:
    void raise_call() noexcept;
    void raise_call_or_calls() noexcept;
    void parse_and_raise_request() noexcept;
};
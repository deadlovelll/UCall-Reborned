#include "helpers/exchange.hpp"
#include "helpers/log.hpp"
#include "helpers/parse.hpp"
#include "helpers/reply.hpp"
#include "helpers/shared.hpp"

struct engine_t {
    descriptor_t socket{};
    struct io_uring uring {};
    bool has_send_zc{};

    std::atomic<std::size_t> active_connections{};
    std::atomic<std::size_t> reserved_connections{};
    std::atomic<std::size_t> dismissed_connections{};
    std::uint32_t max_lifetime_micro_seconds{};
    std::uint32_t max_lifetime_exchanges{};

    mutex_t submission_mutex{};
    mutex_t completion_mutex{};
    mutex_t connections_mutex{};

    stats_t stats{};
    connection_t stats_pseudo_connection{};
    std::int32_t logs_file_descriptor{};
    std::string_view logs_format{};

    /// @brief An array of function callbacks. Can be in dozens.
    array_gt<named_callback_t> callbacks{};
    /// @brief A circular container of reusable connections. Can be in millions.
    pool_gt<connection_t> connections{};
    /// @brief Same number of them, as max physical threads. Can be in hundreds.
    buffer_gt<scratch_space_t> spaces{};
    /// @brief Pre-allocated buffered to be submitted to `io_uring` for shared use.
    memory_map_t fixed_buffers{};

    bool consider_accepting_new_connection() noexcept;
    void submit_stats_heartbeat() noexcept;
    void release_connection(connection_t&) noexcept;
    void log_and_reset_stats() noexcept;

    template <std::size_t max_count_ak>
    std::size_t pop_completed(completed_event_t* events) noexcept {
        unsigned uring_head{};
        unsigned completed{};
        unsigned passed{};
        struct io_uring_cqe* uring_cqe{};

        completion_mutex.lock();
        io_uring_for_each_cqe(&uring, uring_head, uring_cqe) {
            ++passed;
            if (!uring_cqe->user_data)
                continue;
            events[completed].connection_ptr = (connection_t*)uring_cqe->user_data;
            events[completed].stage = events[completed].connection_ptr->stage;
            events[completed].result = uring_cqe->res;
            ++completed;
            if (completed == max_count_ak)
                break;
        }

        io_uring_cq_advance(&uring, passed);
        completion_mutex.unlock();
        return completed;
    }
};
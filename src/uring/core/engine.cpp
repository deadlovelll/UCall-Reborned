#include "engine.hpp"

engine_t::engine_t() noexcept = default;

bool engine_t::consider_accepting_new_connection() noexcept {
    std::size_t reserved_connections_old{};
    if (
        !reserved_connections.compare_exchange_strong(
            reserved_connections_old, 
            1u
        )
    )
        return false;

    connections_mutex.lock();
    connection_t* con_ptr = connections.alloc();
    connections_mutex.unlock();
    if (!con_ptr) {
        dismissed_connections++;
        return false;
    }

    int uring_result{};
    struct io_uring_sqe* uring_sqe{};
    connection_t& connection = *con_ptr;
    connection.stage = stage_t::waiting_to_accept_k;
    submission_mutex.lock();

    uring_sqe = io_uring_get_sqe(&uring);
    io_uring_prep_accept_direct(
        uring_sqe, 
        socket, 
        &connection.client_address, 
        &connection.client_address_len, 
        0,
        IORING_FILE_INDEX_ALLOC
    );
    io_uring_sqe_set_data(uring_sqe, &connection);

    // Accepting new connections can be time-less.
    // io_uring_sqe_set_flags(uring_sqe, IOSQE_IO_LINK);
    // uring_sqe = io_uring_get_sqe(&uring);
    // io_uring_prep_link_timeout(uring_sqe, &connection.next_wakeup, 0);
    // io_uring_sqe_set_data(uring_sqe, NULL);

    uring_result = io_uring_submit(&uring);
    submission_mutex.unlock();
    if (uring_result < 0) {
        connections.release(con_ptr);
        reserved_connections--;
        return false;
    }

    dismissed_connections = 0;
    return true;
}

void engine_t::log_and_reset_stats() noexcept {
    static char printed_message_k[ram_page_size_k]{};
    auto len = logs_format == "json" //
                   ? stats.log_json(printed_message_k, ram_page_size_k)
                   : stats.log_human_readable(
                    printed_message_k, 
                    ram_page_size_k, 
                    stats_t::default_frequency_secs_k
                );
    len = write(logs_file_descriptor, printed_message_k, len);
}

void engine_t::submit_stats_heartbeat() noexcept {
    int uring_result{};
    struct io_uring_sqe* uring_sqe{};
    connection_t& connection = stats_pseudo_connection;
    connection.stage = stage_t::log_stats_k;
    connection.next_wakeup.tv_sec = stats_t::default_frequency_secs_k;
    submission_mutex.lock();

    uring_sqe = io_uring_get_sqe(&uring);
    io_uring_prep_timeout(uring_sqe, &connection.next_wakeup, 0, 0);
    io_uring_sqe_set_data(uring_sqe, &connection);
    uring_result = io_uring_submit(&uring);
    submission_mutex.unlock();
}

void engine_t::release_connection(connection_t& connection) noexcept {
    auto is_active = connection.stage != stage_t::waiting_to_accept_k;
    connection.reset();
    connections_mutex.lock();
    connections.release(&connection);
    connections_mutex.unlock();
    active_connections -= is_active;
    stats.closed_connections.fetch_add(is_active, std::memory_order_relaxed);
}
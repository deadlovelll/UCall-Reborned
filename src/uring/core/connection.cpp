#include "connection.hpp"

connection_t::connection_t() noexcept = default;

bool connection_t::expired() const noexcept {
    if (sleep_ns > max_inactive_duration_ns_k)
        return true;

    return false;
}

void connection_t::reset() noexcept {
    pipes = exchange_pipes_t{};
    descriptor = invalid_descriptor_k;
    stage = stage_t::waiting_to_accept_k;
    std::memset(&client_address, 0, sizeof(client_address));
    client_address_len = sizeof(client_address);
    sleep_ns = 0;
    empty_transmits = 0;
    exchanges = 0;
    next_wakeup = {0, wakeup_initial_frequency_ns_k};
    keep_alive.reset();
    content_length.reset();
    content_type.reset();
}
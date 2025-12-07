#include "helpers/exchange.hpp"
#include "helpers/log.hpp"
#include "helpers/parse.hpp"
#include "helpers/reply.hpp"
#include "helpers/shared.hpp"

struct connection_t {

    /// @brief Exchange buffers to pipe information in both directions.
    exchange_pipes_t pipes{};

    /// @brief The file descriptor of the stateful connection over TCP.
    descriptor_t descriptor{invalid_descriptor_k};
    /// @brief Current state at which the automata has arrived.
    stage_t stage{};

    struct sockaddr client_address {};
    socklen_t client_address_len{sizeof(struct sockaddr)};

    /// @brief Accumulated duration of sleep cycles.
    std::size_t sleep_ns{};
    std::size_t empty_transmits{};
    std::size_t exchanges{};

    /// @brief Relative time set for the last wake-up call.
    struct __kernel_timespec next_wakeup {
        0, wakeup_initial_frequency_ns_k
    };
    /// @brief Absolute time extracted from HTTP headers, 
    /// for the requested lifetime of this channel.
    std::optional<struct __kernel_timespec> keep_alive{};
    /// @brief Expected reception length extracted from HTTP headers.
    std::optional<std::size_t> content_length{};
    /// @brief Expected MIME type of payload extracted 
    /// from HTTP headers. Generally "application/json".
    std::optional<std::string_view> content_type{};

    connection_t() noexcept {}

    bool expired() const noexcept;
    void reset() noexcept;
};
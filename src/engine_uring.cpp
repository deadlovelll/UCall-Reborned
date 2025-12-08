/**
 * @brief JSON-RPC implementation for TCP/IP stack with `io_uring`.
 *
 * Supports:
 * > Thousands of concurrent stateful connections.
 * > Hundreds of physical execution threads.
 * > Both HTTP and HTTP-less raw JSON-RPC calls.
 *
 * @section Primary structures
 * - `engine_t`: primary server instance.
 * - `connection_t`: lifetime state of a single TCP connection.
 * - `scratch_space_t`: temporary memory used by a single thread at a time.
 * - `automata_t`: automata that accepts and responds to messages.
 *
 * @section Concurrency
 * The whole class is thread safe and can be used with as many threads as
 * defined during construction with `ucall_init`. Some `connection_t`-s
 * can, however, be simultaneously handled by two threads, if one logical
 * operation is split into multiple physical calls:
 *
 *      1.  Receiving packets with timeouts.
 *          This allows us to reconsider closing a connection every once
 *          in a while, instead of loyally waiting for more data to come.
 *      2.  Closing sockets gracefully.
 *
 * @section Linux kernel requirements
 * We need Submission Queue Polling to extract maximum performance from `io_uring`.
 * Many of the requests would get an additional `IOSQE_FIXED_FILE` flag, and the
 * setup call would receive `IORING_SETUP_SQPOLL`. Aside from those, we also
 * need to prioritize following efficient interfaces:
 * - `io_uring_prep_accept_direct` to alloc from reusable files list > 5.19..
 * - `io_uring_prep_read_fixed` to read into registered buffers.
 * - `io_uring_register_buffers`.
 * - `io_uring_register_files_sparse` > 5.19, or `io_uring_register_files` before that.
 * - `IORING_SETUP_COOP_TASKRUN` > 5.19.
 * - `IORING_SETUP_SINGLE_ISSUER` > 6.0.
 *
 * @author Ashot Vardanian
 *
 * @see Notable links:
 * https://man7.org/linux/man-pages/dir_by_project.html#liburing
 * https://jvns.ca/blog/2017/06/03/async-io-on-linux--select--poll--and-epoll/
 * https://stackoverflow.com/a/17665015/2766161
 */
#include <arpa/inet.h>  // `inet_addr`
#include <fcntl.h>      // `fcntl`
#include <netinet/in.h> // `sockaddr_in`
#include <stdlib.h>     // `std::aligned_malloc`
#include <sys/ioctl.h>
#include <sys/mman.h>   // `mmap`
#include <sys/socket.h> // `recv`, `setsockopt`
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <liburing.h>

#include <charconv> // `std::to_chars`
#include <mutex>    // `std::mutex`
#include <optional> // `std::optional`

#include <simdjson.h>

#include "ucall/ucall.h"

#include "helpers/exchange.hpp"
#include "helpers/log.hpp"
#include "helpers/parse.hpp"
#include "helpers/reply.hpp"
#include "helpers/shared.hpp"
#include "uring/core/connection.hpp"
#include "uring/core/engine.hpp"
#include "uring/core/memory_map.hpp"
#include "uring/core/automata.hpp"

#pragma region Cpp Declaration

namespace sj = simdjson;
namespace sjd = sj::dom;
using namespace unum::ucall;

/// @brief As we use SIMDJSON, we don't want to fill our message buffers entirely.
/// If there is a @b padding at the end, matching the size of the largest CPU register
/// on the machine, we would avoid copies.
static constexpr std::size_t max_embedded_length_k{ram_page_size_k - sj::SIMDJSON_PADDING};
static constexpr std::size_t sleep_growth_factor_k{4};
static constexpr std::size_t wakeup_initial_frequency_ns_k{3'000};
static constexpr std::size_t max_inactive_duration_ns_k{100'000'000'000};
static constexpr descriptor_t invalid_descriptor_k{-1};

struct completed_event_t;
struct connection_t;
struct engine_t;
struct automata_t;

enum class stage_t {
    waiting_to_accept_k = 0,
    expecting_reception_k,
    responding_in_progress_k,
    waiting_to_close_k,
    log_stats_k,
    unknown_k,
};

struct completed_event_t {
    connection_t* connection_ptr{};
    stage_t stage{};
    int result{};
};

class alignas(align_k) mutex_t {
    std::atomic<bool> flag{false};

  public:
    void lock() noexcept {
        while (flag.exchange(true, std::memory_order_relaxed))
            ;
        std::atomic_thread_fence(std::memory_order_acquire);
    }

    void unlock() noexcept {
        std::atomic_thread_fence(std::memory_order_release);
        flag.store(false, std::memory_order_relaxed);
    }
};

bool io_check_send_zc() noexcept {
    io_uring_probe* probe = io_uring_get_probe();
    if (!probe)
        return false;

    // Available since 6.0.
    bool res = io_uring_opcode_supported(probe, IORING_OP_SEND_ZC);
    io_uring_free_probe(probe);
    return res;
}

sj::simdjson_result<sjd::element> param_at(ucall_call_t call, ucall_str_t name, size_t name_len) noexcept {
    automata_t& automata = *reinterpret_cast<automata_t*>(call);
    scratch_space_t& scratch = automata.scratch;
    name_len = string_length(name, name_len);
    return scratch.point_to_param({name, name_len});
}

sj::simdjson_result<sjd::element> param_at(ucall_call_t call, size_t position) noexcept {
    automata_t& automata = *reinterpret_cast<automata_t*>(call);
    scratch_space_t& scratch = automata.scratch;
    return scratch.point_to_param(position);
}

void ucall_init(ucall_config_t* config_inout, ucall_server_t* server_out) {

    // Simple sanity check
    if (!server_out && !config_inout)
        return;

    // Retrieve configs, if present
    ucall_config_t& config = *config_inout;
    if (!config.port)
        config.port = 8545u;
    if (!config.queue_depth)
        config.queue_depth = 4096u;
    if (!config.max_callbacks)
        config.max_callbacks = 128u;
    if (!config.max_concurrent_connections)
        config.max_concurrent_connections = 1024u;
    if (!config.max_threads)
        config.max_threads = 1u;
    if (!config.max_lifetime_micro_seconds)
        config.max_lifetime_micro_seconds = 100'000u;
    if (!config.max_lifetime_exchanges)
        config.max_lifetime_exchanges = 100u;
    if (!config.hostname)
        config.hostname = "0.0.0.0";

    // Allocate
    int socket_options{1};
    int socket_descriptor{-1};
    int uring_result{-1};
    struct io_uring uring {};
    struct io_uring_params uring_params {};
    struct io_uring_sqe* uring_sqe{};
    struct io_uring_cqe* uring_cqe{};
    uring_params.features |= IORING_FEAT_FAST_POLL;
    uring_params.features |= IORING_FEAT_SQPOLL_NONFIXED;
    // uring_params.flags |= IORING_SETUP_COOP_TASKRUN;
    uring_params.flags |= IORING_SETUP_SQPOLL;
    uring_params.sq_thread_idle = wakeup_initial_frequency_ns_k;
    // uring_params.flags |= config.max_threads == 1 ? IORING_SETUP_SINGLE_ISSUER : 0; // 6.0+
    engine_t* server_ptr{};
    pool_gt<connection_t> connections{};
    array_gt<named_callback_t> callbacks{};
    buffer_gt<scratch_space_t> spaces{};
    buffer_gt<struct iovec> registered_buffers{};
    memory_map_t fixed_buffers{};

    // By default, let's open TCP port for IPv4.
    struct sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(config.hostname);
    address.sin_port = htons(config.port);

    // Initialize `io_uring` first, it is the most likely to fail.
    uring_result = io_uring_queue_init_params(config.queue_depth, &uring, &uring_params);
    if (uring_result != 0)
        goto cleanup;

    // Try allocating all the necessary memory.
    server_ptr = (engine_t*)std::malloc(sizeof(engine_t));
    if (!server_ptr)
        goto cleanup;
    if (!callbacks.reserve(config.max_callbacks))
        goto cleanup;
    if (!fixed_buffers.reserve(ram_page_size_k * 2u * config.max_concurrent_connections))
        goto cleanup;
    if (!connections.reserve(config.max_concurrent_connections))
        goto cleanup;
    if (!spaces.resize(config.max_threads))
        goto cleanup;
    for (auto& space : spaces)
        if (space.parser.allocate(ram_page_size_k, ram_page_size_k / 2u) != sj::SUCCESS)
            goto cleanup;

    // Additional `io_uring` setup.
    if (!registered_buffers.resize(config.max_concurrent_connections * 2u))
        goto cleanup;
    for (std::size_t i = 0; i != config.max_concurrent_connections; ++i) {
        auto& connection = connections.at_offset(i);
        auto inputs = fixed_buffers.ptr + ram_page_size_k * 2u * i;
        auto outputs = inputs + ram_page_size_k;
        connection.pipes.mount(inputs, outputs);

        registered_buffers[i * 2u].iov_base = inputs;
        registered_buffers[i * 2u].iov_len = ram_page_size_k;
        registered_buffers[i * 2u + 1u].iov_base = outputs;
        registered_buffers[i * 2u + 1u].iov_len = ram_page_size_k;
    }
    uring_result = io_uring_register_files_sparse(&uring, config.max_concurrent_connections);
    if (uring_result != 0)
        goto cleanup;
    uring_result =
        io_uring_register_buffers(&uring, registered_buffers.data(), static_cast<unsigned>(registered_buffers.size()));
    if (uring_result != 0)
        goto cleanup;

    // Configure the socket.
    // In the past we would use the normal POSIX call, but we should prefer direct descriptors over it.
    // socket_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    uring_sqe = io_uring_get_sqe(&uring);
    io_uring_prep_socket_direct(uring_sqe, AF_INET, SOCK_STREAM, 0, IORING_FILE_INDEX_ALLOC, 0);
    uring_result = io_uring_submit_and_wait(&uring, 1);
    uring_result = io_uring_wait_cqe(&uring, &uring_cqe);
    socket_descriptor = uring_cqe->res;
    if (socket_descriptor < 0)
        goto cleanup;
    // Not sure if this is required, after we have a kernel with `IORING_OP_SENDMSG_ZC` support, we can check.
    // if (setsockopt(socket_descriptor, SOL_SOCKET, SO_ZEROCOPY, &socket_options, sizeof(socket_options)) == -1)
    //     goto cleanup;
    if (bind(socket_descriptor, (struct sockaddr*)&address, sizeof(address)) < 0)
        goto cleanup;
    if (listen(socket_descriptor, config.queue_depth) < 0)
        goto cleanup;

    // Initialize all the members.
    new (server_ptr) engine_t();
    server_ptr->socket = descriptor_t{socket_descriptor};
    server_ptr->max_lifetime_micro_seconds = config.max_lifetime_micro_seconds;
    server_ptr->max_lifetime_exchanges = config.max_lifetime_exchanges;
    server_ptr->callbacks = std::move(callbacks);
    server_ptr->connections = std::move(connections);
    server_ptr->spaces = std::move(spaces);
    server_ptr->uring = uring;
    server_ptr->has_send_zc = io_check_send_zc();
    server_ptr->logs_file_descriptor = config.logs_file_descriptor;
    server_ptr->logs_format = config.logs_format ? std::string_view(config.logs_format) : std::string_view();
    *server_out = (ucall_server_t)server_ptr;
    return;

cleanup:
    errno;
    if (uring.ring_fd)
        io_uring_queue_exit(&uring);
    if (socket_descriptor >= 0)
        close(socket_descriptor);
    std::free(server_ptr);
    *server_out = nullptr;
}

void ucall_add_procedure(
    ucall_server_t server, 
    ucall_str_t name, 
    ucall_callback_t callback,
    ucall_callback_tag_t callback_tag
) {
    engine_t& engine = *reinterpret_cast<engine_t*>(server);
    if (
        engine.callbacks.size() + 1 
        < engine.callbacks.capacity()
    )
        engine.callbacks.push_back_reserved({name, callback, callback_tag});
}

void ucall_free(ucall_server_t server) {
    if (!server)
        return;

    engine_t& engine = *reinterpret_cast<engine_t*>(server);
    io_uring_unregister_buffers(&engine.uring);
    io_uring_queue_exit(&engine.uring);
    close(engine.socket);
    engine.~engine_t();
    std::free(server);
}

void ucall_take_calls(ucall_server_t server, uint16_t thread_idx) {
    engine_t& engine = *reinterpret_cast<engine_t*>(server);
    if (!thread_idx && engine.logs_file_descriptor > 0)
        engine.submit_stats_heartbeat();
    while (true) {
        ucall_take_call(server, thread_idx);
    }
}

void ucall_take_call(ucall_server_t server, uint16_t thread_idx) {
    // Unlike the classical synchronous interface, this implements only a part of the connection machine,
    // is responsible for checking if a specific request has been completed. All of the submitted
    // memory must be preserved until we get the confirmation.
    engine_t& engine = *reinterpret_cast<engine_t*>(server);
    if (!thread_idx)
        engine.consider_accepting_new_connection();

    constexpr std::size_t completed_max_k{16};
    completed_event_t completed_events[completed_max_k]{};
    std::size_t completed_count = engine.pop_completed<completed_max_k>(completed_events);

    for (std::size_t i = 0; i != completed_count; ++i) {
        completed_event_t& completed = completed_events[i];
        automata_t automata{
            engine, //
            engine.spaces[thread_idx],
            *completed.connection_ptr,
            completed.stage,
            completed.result,
        };

        // If everything is fine, let automata work in its normal regime.
        automata();
    }
}

void ucall_call_reply_content(ucall_call_t call, ucall_str_t body, size_t body_len) {
    automata_t& automata = *reinterpret_cast<automata_t*>(call);
    connection_t& connection = automata.connection;
    scratch_space_t& scratch = automata.scratch;
    // No response is needed for "id"-less notifications.
    if (scratch.dynamic_id.empty())
        return;

    body_len = string_length(body, body_len);
    struct iovec iovecs[iovecs_for_content_k] {};
    fill_with_content(iovecs, scratch.dynamic_id, std::string_view(body, body_len), true);
    connection.pipes.append_outputs<iovecs_for_content_k>(iovecs);
}

void ucall_call_reply_error(ucall_call_t call, int code_int, ucall_str_t note, size_t note_len) {
    automata_t& automata = *reinterpret_cast<automata_t*>(call);
    connection_t& connection = automata.connection;
    scratch_space_t& scratch = automata.scratch;
    // No response is needed for "id"-less notifications.
    if (scratch.dynamic_id.empty())
        return;

    note_len = string_length(note, note_len);
    char code[max_integer_length_k]{};
    std::to_chars_result res = std::to_chars(code, code + max_integer_length_k, code_int);
    auto code_len = res.ptr - code;
    if (res.ec != std::error_code())
        return ucall_call_reply_error_unknown(call);

    struct iovec iovecs[iovecs_for_error_k] {};
    fill_with_error(iovecs, scratch.dynamic_id, std::string_view(code, code_len), std::string_view(note, note_len),
                    true);
    if (!connection.pipes.append_outputs<iovecs_for_error_k>(iovecs))
        return ucall_call_reply_error_out_of_memory(call);
}

void ucall_call_reply_error_invalid_params(ucall_call_t call) {
    return ucall_call_reply_error(call, -32602, "Invalid method param(s).", 24);
}

void ucall_call_reply_error_unknown(ucall_call_t call) {
    return ucall_call_reply_error(call, -32603, "Unknown error.", 14);
}

void ucall_call_reply_error_out_of_memory(ucall_call_t call) {
    return ucall_call_reply_error(call, -32000, "Out of memory.", 14);
}

bool ucall_param_named_bool(ucall_call_t call, ucall_str_t name, size_t name_len, bool* result_ptr) {
    if (auto value = param_at(call, name, name_len); value.is_bool()) {
        *result_ptr = value.get_bool().value_unsafe();
        return true;
    } else
        return false;
}

bool ucall_param_named_i64(ucall_call_t call, ucall_str_t name, size_t name_len, int64_t* result_ptr) {
    if (auto value = param_at(call, name, name_len); value.is_int64()) {
        *result_ptr = value.get_int64().value_unsafe();
        return true;
    } else
        return false;
}

bool ucall_param_named_f64(ucall_call_t call, ucall_str_t name, size_t name_len, double* result_ptr) {
    if (auto value = param_at(call, name, name_len); value.is_double()) {
        *result_ptr = value.get_double().value_unsafe();
        return true;
    } else
        return false;
}

bool ucall_param_named_str(ucall_call_t call, ucall_str_t name, size_t name_len, ucall_str_t* result_ptr,
                           size_t* result_len_ptr) {
    if (auto value = param_at(call, name, name_len); value.is_string()) {
        *result_ptr = value.get_string().value_unsafe().data();
        *result_len_ptr = value.get_string_length().value_unsafe();
        return true;
    } else
        return false;
}

bool ucall_param_positional_bool(ucall_call_t call, size_t position, bool* result_ptr) {
    if (auto value = param_at(call, position); value.is_bool()) {
        *result_ptr = value.get_bool().value_unsafe();
        return true;
    } else
        return false;
}

bool ucall_param_positional_i64(ucall_call_t call, size_t position, int64_t* result_ptr) {
    if (auto value = param_at(call, position); value.is_int64()) {
        *result_ptr = value.get_int64().value_unsafe();
        return true;
    } else
        return false;
}

bool ucall_param_positional_f64(ucall_call_t call, size_t position, double* result_ptr) {
    if (auto value = param_at(call, position); value.is_double()) {
        *result_ptr = value.get_double().value_unsafe();
        return true;
    } else
        return false;
}

bool ucall_param_positional_str(ucall_call_t call, size_t position, ucall_str_t* result_ptr, size_t* result_len_ptr) {
    if (auto value = param_at(call, position); value.is_string()) {
        *result_ptr = value.get_string().value_unsafe().data();
        *result_len_ptr = value.get_string_length().value_unsafe();
        return true;
    } else
        return false;
}
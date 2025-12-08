#include "automata.hpp"

automata_t::automata_t() noexcept = default;

bool automata_t::should_release() const noexcept {
    return connection.expired() || engine.dismissed_connections || connection.empty_transmits > 100;
}

bool automata_t::received_full_request() const noexcept {
    auto span = connection.pipes.input_span();
    // if (!connection.content_length) {
    size_t bytes_expected = 0;

    auto json_or_error = split_body_headers(std::string_view(span.data(), span.size()));
    if (auto error_ptr = std::get_if<default_error_t>(&json_or_error); error_ptr)
        return true;
    parsed_request_t request = std::get<parsed_request_t>(json_or_error);

    auto res = std::from_chars(
        request.content_length.begin(), 
        request.content_length.end(), 
        bytes_expected
    );
    bytes_expected += (request.body.begin() - span.data());

    if (res.ec == std::errc::invalid_argument || bytes_expected <= 0)
        // TODO: Maybe not a HTTP request, What to do?
        return true;

    connection.content_length = bytes_expected;
    // }

    if (span.size() < *connection.content_length)
        return false;

    return true;
}

void automata_t::raise_call() noexcept {
    auto callback_or_error = find_callback(engine.callbacks, scratch);
    if (auto error_ptr = std::get_if<default_error_t>(&callback_or_error); error_ptr)
        return ucall_call_reply_error(this, error_ptr->code, error_ptr->note.data(), error_ptr->note.size());

    named_callback_t named_callback = std::get<named_callback_t>(callback_or_error);
    return named_callback.callback(this, named_callback.callback_tag);
}

void automata_t::raise_call_or_calls() noexcept {
    exchange_pipes_t& pipes = connection.pipes;
    sjd::parser& parser = *scratch.dynamic_parser;
    std::string_view json_body = scratch.dynamic_packet;
    parser.set_max_capacity(json_body.size());
    auto one_or_many = parser.parse(json_body.data(), json_body.size(), false);
    if (one_or_many.error() == sj::CAPACITY)
        return ucall_call_reply_error_out_of_memory(this);

    // We may need to prepend the response with HTTP headers.
    if (scratch.is_http)
        pipes.append_reserved(http_header_k, http_header_size_k);

    size_t body_size = pipes.output_span().size();

    if (one_or_many.error() != sj::SUCCESS)
        return ucall_call_reply_error(this, -32700, "Invalid JSON was received by the server.", 40);

    // Check if we hve received a batch request.
    else if (one_or_many.is_array()) {
        sjd::array many = one_or_many.get_array().value_unsafe();
        scratch.is_batch = true;

        // Start a JSON array. Originally it must fit into `embedded` part.
        pipes.push_back_reserved('[');

        for (sjd::element const one : many) {
            scratch.tree = one;
            raise_call();
        }

        // Replace the last comma with the closing bracket.
        pipes.output_pop_back();
        pipes.push_back_reserved(']');
    }
    // This is a single request
    else {
        scratch.is_batch = false;
        scratch.tree = one_or_many.value_unsafe();
        raise_call();

        if (scratch.dynamic_id.empty()) {
            pipes.push_back_reserved('{');
            pipes.push_back_reserved('}');
        } else if (pipes.has_outputs()) // Drop the last comma, if present.
            pipes.output_pop_back();
    }

    // Now, as we know the length of the whole response, we can update
    // the HTTP headers to indicate thr real "Content-Length".
    if (scratch.is_http) {
        auto output = pipes.output_span();
        body_size = output.size() - body_size;
        if (!set_http_content_length(output.data(), body_size))
            return ucall_call_reply_error_out_of_memory(this);
    }
}

void automata_t::parse_and_raise_request() noexcept {
    auto request = connection.pipes.input_span();
    auto parsed_request_or_error = split_body_headers(request);
    if (auto error_ptr = std::get_if<default_error_t>(&parsed_request_or_error); error_ptr)
        // TODO: This error message may have to be wrapped into an HTTP header separately
        return ucall_call_reply_error(this, error_ptr->code, error_ptr->note.data(), error_ptr->note.size());

    auto parsed_request = std::get<parsed_request_t>(parsed_request_or_error);
    scratch.is_http = request.size() != parsed_request.body.size();
    scratch.dynamic_packet = parsed_request.body;
    if (scratch.dynamic_packet.size() > ram_page_size_k) {
        sjd::parser parser;
        if (parser.allocate(scratch.dynamic_packet.size(), scratch.dynamic_packet.size() / 2) != sj::SUCCESS)
            return ucall_call_reply_error_out_of_memory(this);
        else {
            scratch.dynamic_parser = &parser;
            return raise_call_or_calls();
        }
    } else {
        scratch.dynamic_parser = &scratch.parser;
        return raise_call_or_calls();
    }
}

void automata_t::close_gracefully() noexcept {
    int uring_result{};
    struct io_uring_sqe* uring_sqe{};
    connection.stage = stage_t::waiting_to_close_k;

    // The operations are not expected to complete in exactly the same order
    // as their submissions. So to stop all existing communication on the
    // socket, we can cancel everything related to its "file descriptor",
    // and then close.
    engine.submission_mutex.lock();
    uring_sqe = io_uring_get_sqe(&engine.uring);
    io_uring_prep_cancel_fd(uring_sqe, int(connection.descriptor), 0);
    io_uring_sqe_set_data(uring_sqe, NULL);
    io_uring_sqe_set_flags(uring_sqe, IOSQE_IO_HARDLINK);

    uring_sqe = io_uring_get_sqe(&engine.uring);
    io_uring_prep_shutdown(uring_sqe, int(connection.descriptor), SHUT_WR);
    io_uring_sqe_set_data(uring_sqe, NULL);
    io_uring_sqe_set_flags(uring_sqe, IOSQE_IO_HARDLINK);

    uring_sqe = io_uring_get_sqe(&engine.uring);
    io_uring_prep_close(uring_sqe, int(connection.descriptor));
    io_uring_sqe_set_data(uring_sqe, &connection);
    io_uring_sqe_set_flags(uring_sqe, 0);

    uring_result = io_uring_submit(&engine.uring);
    engine.submission_mutex.unlock();
}

void automata_t::send_next() noexcept {
    exchange_pipes_t& pipes = connection.pipes;
    int uring_result{};
    struct io_uring_sqe* uring_sqe{};
    connection.stage = stage_t::responding_in_progress_k;
    pipes.release_inputs();

    // TODO: Test and benchmark the `send_zc option`.
    engine.submission_mutex.lock();
    uring_sqe = io_uring_get_sqe(&engine.uring);
    if (engine.has_send_zc) {
        io_uring_prep_send_zc_fixed(uring_sqe, int(connection.descriptor), (void*)pipes.next_output_address(),
                                    pipes.next_output_length(), 0, 0,
                                    engine.connections.offset_of(connection) * 2u + 1u);
    } else {
        io_uring_prep_send(uring_sqe, int(connection.descriptor), (void*)pipes.next_output_address(),
                           pipes.next_output_length(), 0);
        uring_sqe->flags |= IOSQE_FIXED_FILE;
        uring_sqe->buf_index = engine.connections.offset_of(connection) * 2u + 1u;
    }
    io_uring_sqe_set_data(uring_sqe, &connection);
    io_uring_sqe_set_flags(uring_sqe, 0);
    uring_result = io_uring_submit(&engine.uring);
    engine.submission_mutex.unlock();
}

void automata_t::receive_next() noexcept {
    exchange_pipes_t& pipes = connection.pipes;
    int uring_result{};
    struct io_uring_sqe* uring_sqe{};
    connection.stage = stage_t::expecting_reception_k;
    pipes.release_outputs();

    engine.submission_mutex.lock();

    // Choosing between `recv` and `read` system calls:
    // > If a zero-length datagram is pending, read(2) and recv() with a
    // > flags argument of zero provide different behavior. In this
    // > circumstance, read(2) has no effect (the datagram remains
    // > pending), while recv() consumes the pending datagram.
    // https://man7.org/linux/man-pages/man2/recv.2.html
    //
    // In this case we are waiting for an actual data, not some artificial wakeup.
    uring_sqe = io_uring_get_sqe(&engine.uring);
    io_uring_prep_read_fixed(uring_sqe, int(connection.descriptor), (void*)pipes.next_input_address(),
                             pipes.next_input_length(), 0, engine.connections.offset_of(connection) * 2u);
    io_uring_sqe_set_data(uring_sqe, &connection);
    io_uring_sqe_set_flags(uring_sqe, IOSQE_IO_LINK);

    // More than other operations this depends on the information coming from the client.
    // We can't afford to keep connections alive indefinitely, so we need to set a timeout
    // on this operation.
    // The `io_uring_prep_link_timeout` is a convenience method for poorly documented `IORING_OP_LINK_TIMEOUT`.
    uring_sqe = io_uring_get_sqe(&engine.uring);
    io_uring_prep_link_timeout(uring_sqe, &connection.next_wakeup, 0);
    io_uring_sqe_set_data(uring_sqe, NULL);
    io_uring_sqe_set_flags(uring_sqe, 0);
    uring_result = io_uring_submit(&engine.uring);

    engine.submission_mutex.unlock();
}

void automata_t::operator()() noexcept {

    if (is_corrupted())
        return close_gracefully();

    switch (connection.stage) {

    case stage_t::waiting_to_accept_k:

        if (completed_result == -ECANCELED) {
            engine.release_connection(connection);
            engine.reserved_connections--;
            engine.consider_accepting_new_connection();
            return;
        }

        // Check if accepting the new connection request worked out.
        engine.reserved_connections--;
        engine.active_connections++;
        engine.stats.added_connections.fetch_add(1, std::memory_order_relaxed);
        connection.descriptor = descriptor_t{completed_result};
        return receive_next();

    case stage_t::expecting_reception_k:

        // From documentation:
        // > If used, the timeout specified in the command will cancel the linked command,
        // > unless the linked command completes before the timeout. The timeout will complete
        // > with -ETIME if the timer expired and the linked request was attempted canceled,
        // > or -ECANCELED if the timer got canceled because of completion of the linked request.
        //
        // So we expect only two outcomes here:
        // 1. reception expired with: `ECANCELED`, and subsequent timeout expired with `ETIME`.
        // 2. reception can continue and subsequent timer returned `ECANCELED`.
        //
        // If the following timeout request has happened,
        // we don't want to do anything here. Let's leave the faith of
        // this connection to the subsequent timer to decide.
        if (completed_result == -ECANCELED) {
            connection.sleep_ns += connection.next_wakeup.tv_nsec;
            connection.next_wakeup.tv_nsec *= sleep_growth_factor_k;
            completed_result = 0;
        }

        // No data was received.
        if (completed_result == 0) {
            connection.empty_transmits++;
            return should_release() ? close_gracefully() : receive_next();
        }

        // Absorb the arrived data.
        engine.stats.bytes_received.fetch_add(completed_result, std::memory_order_relaxed);
        engine.stats.packets_received.fetch_add(1, std::memory_order_relaxed);
        connection.empty_transmits = 0;
        connection.sleep_ns = 0;
        if (!connection.pipes.absorb_input(completed_result)) {
            ucall_call_reply_error_out_of_memory(this);
            return send_next();
        }

        // If we have reached the end of the stream,
        // it is time to analyze the contents
        // and send back a response.
        if (received_full_request()) {
            parse_and_raise_request();
            connection.pipes.release_inputs();
            // Some requests require no response at all,
            // so we can go back to listening the port.
            if (!connection.pipes.has_outputs()) {
                connection.exchanges++;
                if (connection.exchanges >= engine.max_lifetime_exchanges)
                    return close_gracefully();
                else
                    return receive_next();
            } else {
                connection.pipes.prepare_more_outputs();
                return send_next();
            }
        }
        // We are looking for more data to come
        else if (connection.pipes.shift_input_to_dynamic()) {
            return receive_next();
        }
        // We may fail to allocate memory to receive the next input
        else {
            ucall_call_reply_error_out_of_memory(this);
            return send_next();
        }

    case stage_t::responding_in_progress_k:

        connection.empty_transmits = completed_result == 0 ? ++connection.empty_transmits : 0;

        if (should_release())
            return close_gracefully();

        engine.stats.bytes_sent.fetch_add(completed_result, std::memory_order_relaxed);
        engine.stats.packets_sent.fetch_add(1, std::memory_order_relaxed);
        connection.pipes.mark_submitted_outputs(completed_result);
        if (!connection.pipes.has_remaining_outputs()) {
            connection.exchanges++;
            if (connection.exchanges >= engine.max_lifetime_exchanges)
                return close_gracefully();
            else
                return receive_next();
        } else {
            connection.pipes.prepare_more_outputs();
            return send_next();
        }

    case stage_t::waiting_to_close_k:
        return engine.release_connection(connection);

    case stage_t::log_stats_k:
        engine.log_and_reset_stats();
        return engine.submit_stats_heartbeat();

    case stage_t::unknown_k:
        return;
    }
}
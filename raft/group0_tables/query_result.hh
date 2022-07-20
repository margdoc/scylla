/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <variant>

#include "bytes.hh"
#include "seastar/core/shared_ptr.hh"
#include "transport/messages/result_message_base.hh"


namespace raft::group0_tables {

// Represents result of single cell query.
struct query_result_select {
    bytes_opt value;
};

// Represents result of conditional update query.
struct query_result_conditional_update {
    bool is_applied;
    bytes_opt previous_value;
};

struct query_result_none {};

using query_result = std::variant<query_result_select, query_result_conditional_update, query_result_none>;

::shared_ptr<cql_transport::messages::result_message> to_cql_result(query_result result);

} // namespace raft::group0_tables
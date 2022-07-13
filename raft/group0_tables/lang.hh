/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <variant>

#include "bytes.hh"
#include "cql3/cql_statement.hh"
#include "seastar/core/future.hh"
#include "transport/messages/result_message_base.hh"


namespace service {
    class raft_group0_client;
}

namespace raft::group0_tables {

// Represents 'SELECT value WHERE key = {key} FROM system.group0_kv_store;'.
struct select_query {
    bytes key;
};

// Represents 'UPDATE system.group0_kv_store SET value = {new_value} WHERE key = {key} [IF value = {value_condition}];'.
// If value_condition is nullopt, the update is unconditional.
struct update_query {
    bytes key;
    bytes new_value;
    std::optional<bytes_opt> value_condition;
};

struct query {
    std::variant<select_query, update_query> q;
};

// Function checks if statement should be executed on group0 table.
// For now it returns true if and only if target table is system.group0_kv_store and
// doesn't selects the entire table (it's used for debug).
bool is_group0_table_statement(const cql3::cql_statement& statement);

future<::shared_ptr<cql_transport::messages::result_message>> execute(service::raft_group0_client& group0_client, const cql3::cql_statement& statement);

} // namespace raft::group0_tables

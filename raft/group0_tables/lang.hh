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
#include "exceptions/exceptions.hh"
#include "utils/managed_bytes.hh"


namespace raft::group0_tables {

using namespace std::string_literals;

// Represents 'SELECT value WHERE key = {key} FROM system.group0_kv_store;'.
struct select_query {
    managed_bytes key;
};

// Represents 'UPDATE system.group0_kv_store SET value = {new_value} WHERE key = {key} [IF value = {value_condition}];'.
// If value_condition is nullopt, the update is unconditional.
struct update_query {
    managed_bytes key;
    managed_bytes new_value;
    std::optional<managed_bytes_opt> value_condition;
};

struct query {
    std::variant<select_query, update_query> q;
};

// Function checks if statement should be executed on group0 table.
// For now it returns true if and only if target table is system.group0_kv_store.
bool is_group0_table_statement(const sstring& keyspace, const sstring& column_family);

class unsupported_operation_error : public exceptions::invalid_request_exception {
public:
    unsupported_operation_error()
        : exceptions::invalid_request_exception("currently unsupported operation on group0_kv_store") {
    }

    template<typename S, typename... Args>
    unsupported_operation_error(const S& format_str, Args&&... args)
        : exceptions::invalid_request_exception(fmt::format("currently unsupported operation on group0_kv_store: "s + format_str, std::forward<Args>(args)...)) {
    }
};

} // namespace raft::group0_tables

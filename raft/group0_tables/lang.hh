/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <string>
#include <variant>


namespace raft::group0_tables {

// Represents 'SELECT value WHERE key = {key} FROM system.group0_kv_store;'.
struct select_query {
    std::string key;
};

// Represents 'UPDATE system.group0_kv_store SET value = {new_value} WHERE key = {key} [IF value = {value_condition}];'.
// If value_condition is nullopt, the update is unconditional.
struct update_query {
    std::string key;
    std::string new_value;
    std::optional<std::string> value_condition;
};

struct query {
    std::variant<select_query, update_query> q;
};

} // namespace raft::group0_tables

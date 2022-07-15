/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace raft {
namespace group0_tables {

struct select_query {
    std::string key;
};

struct update_query {
    std::string key;
    std::string new_value;
    std::optional<std::string> value_condition;
};

struct query {
    std::variant<raft::group0_tables::select_query, raft::group0_tables::update_query> q;
};

} // namespace group0_tables
} // namespace raft

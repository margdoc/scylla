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
    std::variant<select_query, update_query> q;
};

} // namespace group0_tables
} // namespace raft

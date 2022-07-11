/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace raft {
namespace group0_tables {

struct select_query {
    managed_bytes key;
};

struct update_query {
    managed_bytes key;
    managed_bytes new_value;
    std::optional<managed_bytes_opt> value_condition;
};

struct query {
    std::variant<select_query, update_query> q;
};

} // namespace group0_tables
} // namespace raft

/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace raft {
namespace group0_tables {

struct select_query {
    bytes key;
};

struct update_query {
    bytes key;
    bytes new_value;
    std::optional<bytes_opt> value_condition;
};

struct query {
    std::variant<select_query, update_query> q;
};

} // namespace group0_tables
} // namespace raft

/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */


#include "cql3/statements/strongly_consistent_modification_statement.hh"
#include "cql3/attributes.hh"
#include "seastar/core/future.hh"

namespace cql3 {

namespace statements {

strongly_consistent_modification_statement::strongly_consistent_modification_statement(
        statement_type type,
        uint32_t bound_terms,
        schema_ptr s,
        std::unique_ptr<attributes> attrs,
        cql_stats& stats)
    : modification_statement{type, bound_terms, std::move(s), std::move(attrs), stats}
{ }

future<::shared_ptr<cql_transport::messages::result_message>>
strongly_consistent_modification_statement::execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options) const {
    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
}

void strongly_consistent_modification_statement::prepare_raft_command() const {
    
}

bool strongly_consistent_modification_statement::require_full_clustering_key() const {
    return true;
}

bool strongly_consistent_modification_statement::allow_clustering_key_slices() const {
    return false;
}

void strongly_consistent_modification_statement::add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) const {
    // empty
}


}

}

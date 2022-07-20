/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "query_result.hh"

#include "cql3/column_identifier.hh"
#include "db/system_keyspace.hh"
#include "seastar/core/shared_ptr.hh"
#include "transport/messages/result_message.hh"
#include "types.hh"
#include "utils/overloaded_functor.hh"

namespace raft::group0_tables {

::shared_ptr<cql_transport::messages::result_message> to_cql_result(query_result result) {
    return std::visit<::shared_ptr<cql_transport::messages::result_message>>(overloaded_functor {
        [] (const query_result_select& qr) {
            auto result_set = std::make_unique<cql3::result_set>(std::vector{
                make_lw_shared<cql3::column_specification>(
                    db::system_keyspace::NAME,
                    db::system_keyspace::GROUP0_KV_STORE,
                    ::make_shared<cql3::column_identifier>("value", true),
                    utf8_type
                )
            });

            if (qr.value) {
                result_set->add_row({ qr.value });
            }

            return ::make_shared<cql_transport::messages::result_message::rows>(cql3::result{std::move(result_set)});
        },
        [] (const query_result_conditional_update& qr) {
            auto result_set = std::make_unique<cql3::result_set>(std::vector{
                make_lw_shared<cql3::column_specification>(
                    db::system_keyspace::NAME,
                    db::system_keyspace::GROUP0_KV_STORE,
                    ::make_shared<cql3::column_identifier>("[applied]", false),
                    boolean_type
                ),
                make_lw_shared<cql3::column_specification>(
                    db::system_keyspace::NAME,
                    db::system_keyspace::GROUP0_KV_STORE,
                    ::make_shared<cql3::column_identifier>("value", true),
                    utf8_type
                )
            });

            result_set->add_row({ boolean_type->decompose(qr.is_applied), qr.previous_value });

            return ::make_shared<cql_transport::messages::result_message::rows>(cql3::result{std::move(result_set)});
        },
        [] (const query_result_none&) {
            return ::shared_ptr<cql_transport::messages::result_message>{};
        }
    }, result);
}

}
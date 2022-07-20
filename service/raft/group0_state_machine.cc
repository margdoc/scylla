/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/group0_state_machine.hh"
#include <fmt/core.h>
#include <optional>
#include <seastar/core/coroutine.hh>
#include "service/migration_manager.hh"
#include "atomic_cell.hh"
#include "bytes.hh"
#include "cql3/result_generator.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "cql3/stats.hh"
#include "dht/i_partitioner.hh"
#include "dht/token.hh"
#include "message/messaging_service.hh"
#include "canonical_mutation.hh"
#include "concrete_types.hh"
#include "mutation_partition.hh"
#include "raft/group0_tables/lang.hh"
#include "raft/group0_tables/query_result.hh"
#include "schema.hh"
#include "schema_mutations.hh"
#include "frozen_schema.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sharded.hh"
#include "seastarx.hh"
#include "serialization_visitors.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/group0_tables_lang.dist.hh"
#include "idl/group0_tables_lang.dist.impl.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"
#include "service/migration_manager.hh"
#include "db/system_keyspace.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_group0_client.hh"
#include "partition_slice_builder.hh"
#include "types.hh"
#include "utils/overloaded_functor.hh"
#include "query-result-reader.hh"
#include "transport/messages/result_message.hh"

namespace service {

static logging::logger slogger("group0_raft_sm");

static mutation extract_history_mutation(std::vector<canonical_mutation>& muts, const data_dictionary::database db) {
    auto s = db.find_schema(db::system_keyspace::NAME, db::system_keyspace::GROUP0_HISTORY);
    auto it = std::find_if(muts.begin(), muts.end(), [history_table_id = s->id()]
            (canonical_mutation& m) { return m.column_family_id() == history_table_id; });
    if (it == muts.end()) {
        on_internal_error(slogger, "group0 history table mutation not found");
    }
    auto res = it->to_mutation(s);
    muts.erase(it);
    return res;
}

static mutation convert_history_mutation(canonical_mutation m, const data_dictionary::database db) {
    return m.to_mutation(db.find_schema(db::system_keyspace::NAME, db::system_keyspace::GROUP0_HISTORY));
}

static
std::pair<lw_shared_ptr<query::read_command>, dht::partition_range>
prepare_read_command(storage_proxy& proxy, const schema& schema, const std::string& key) {
    auto slice = partition_slice_builder(schema).build();
    auto partition_key = partition_key::from_single_value(schema, to_bytes(key));
    dht::ring_position ring_position(dht::get_token(schema, partition_key), partition_key);
    auto range = dht::partition_range::make_singular(ring_position);
    return {make_lw_shared<query::read_command>(schema.id(), schema.version(), slice, proxy.get_max_result_size(slice)), range};
}

static
atomic_cell_view get_cell(
    mutation& mutation,
    const schema_ptr& schema) {
    const auto* column = schema->get_column_definition("value");
    return mutation.partition().clustered_row(*schema, clustering_key::make_empty()).cells().cell_at(column->id).as_atomic_cell(*column);
}

static future<raft::group0_tables::query_result> execute_group0_table_query(
    storage_proxy& proxy,
    const table_query& query,
    const service::group0_command& cmd) {
    return std::visit(overloaded_functor {
    [&] (const raft::group0_tables::select_query& q) -> future<raft::group0_tables::query_result> {
        const auto schema = db::system_keyspace::group0_kv_store();

        // Read mutations
        const auto [read_cmd, range] = prepare_read_command(proxy, *schema, q.key);
        auto [rs, _] = co_await proxy.query_mutations_locally(schema, read_cmd, range, db::no_timeout);

        if (rs->partitions().empty()) {
            co_return raft::group0_tables::query_result_select{};
        }

        assert(rs->partitions().size() == 1); // In this version only one value per partition key is allowed.

        const auto& p = rs->partitions()[0];
        auto mutation = p.mut().unfreeze(schema);
        const auto cell = get_cell(mutation, schema);

        co_return raft::group0_tables::query_result_select{
                .value = cell.value().linearize()
            };
    },
    [&] (const raft::group0_tables::update_query& q) -> future<raft::group0_tables::query_result> {
        const auto& _cmd = cmd;
        auto& _proxy = proxy;

        const auto schema = db::system_keyspace::group0_kv_store();

        // Read mutations
        const auto [read_cmd, range] = prepare_read_command(proxy, *schema, q.key);
        auto [rs, _] = co_await proxy.query_mutations_locally(schema, read_cmd, range, db::no_timeout);

        if (!rs->partitions().empty()) {
            assert(rs->partitions().size() == 1); // In this version only one value per partition key is allowed.

            auto& p = rs->partitions()[0];
            auto mutation = p.mut().unfreeze(schema);
            auto cell = get_cell(mutation, schema);

            if (!q.value_condition || to_bytes(*q.value_condition) == cell.value().linearize()) {
                auto old_ts = cell.timestamp();
                auto from_state_id = utils::UUID_gen::micros_timestamp(_cmd.new_state_id);
                auto ts = std::max(from_state_id, old_ts + 1);

                mutation.set_clustered_cell(clustering_key::make_empty(), "value", data_value(q.new_value), ts);
                co_await _proxy.mutate_locally(mutation, {}, {});
            }
        } else {
            if (!q.value_condition) {
                mutation mutation(schema, partition_key::from_single_value(*schema, to_bytes(q.key)));
                auto ts = utils::UUID_gen::micros_timestamp(_cmd.new_state_id);

                mutation.set_clustered_cell(clustering_key::make_empty(), "value", data_value(q.new_value), ts);
                co_await _proxy.mutate_locally(mutation, {}, {});
            }
        }

        co_return raft::group0_tables::query_result_none{};
    }
    }, query.query.q);
}

future<> group0_state_machine::apply(std::vector<raft::command_cref> command) {
    slogger.trace("apply() is called");
    for (auto&& c : command) {
        auto is = ser::as_input_stream(c);
        auto cmd = ser::deserialize(is, boost::type<group0_command>{});

        slogger.trace("cmd: prev_state_id: {}, new_state_id: {}, creator_addr: {}, creator_id: {}",
                cmd.prev_state_id, cmd.new_state_id, cmd.creator_addr, cmd.creator_id);
        slogger.trace("cmd.history_append: {}", cmd.history_append);

        auto read_apply_mutex_holder = co_await get_units(_client._read_apply_mutex, 1);

        if (cmd.prev_state_id) {
            auto last_group0_state_id = co_await db::system_keyspace::get_last_group0_state_id();
            if (*cmd.prev_state_id != last_group0_state_id) {
                // This command used obsolete state. Make it a no-op.
                // BTW. on restart, all commands after last snapshot descriptor become no-ops even when they originally weren't no-ops.
                // This is because we don't restart from snapshot descriptor, but using current state of the tables so the last state ID
                // is the one given by the last command.
                // Similar thing may happen when we pull group0 state in transfer_snapshot - we pull the latest state of remote tables,
                // not state at the snapshot descriptor.
                slogger.trace("cmd.prev_state_id ({}) different than last group 0 state ID in history table ({})",
                        cmd.prev_state_id, last_group0_state_id);
                continue;
            }
        } else {
            slogger.trace("unconditional modification, cmd.new_state_id: {}", cmd.new_state_id);
        }

        // We assume that `cmd.change` was constructed using group0 state which was observed *after* `cmd.prev_state_id` was obtained.
        // It is now important that we apply the change *before* we append the group0 state ID to the history table.
        //
        // If we crash before appending the state ID, when we reapply the command after restart, the change will be applied because
        // the state ID was not yet appended so the above check will pass.

        // TODO: reapplication of a command after a crash may require contacting a quorum (we need to learn that the command
        // is committed from a leader). But we may want to ensure that group 0 state is consistent after restart even without
        // access to quorum, which means we cannot allow partially applied commands. We need to ensure that either the entire
        // change is applied and the state ID is updated or none of this happens.
        // E.g. use a write-ahead-entry which contains all this information and make sure it's replayed during restarts.

        co_await std::visit(make_visitor(
        [&] (schema_change& chng) -> future<> {
            return _mm.merge_schema_from(netw::messaging_service::msg_addr(std::move(cmd.creator_addr)), std::move(chng.mutations));
        },
        [&] (table_query& query) -> future<> {
            auto& client = _client;
            auto result = co_await execute_group0_table_query(_sp, query, cmd);
            client.set_query_result(cmd.new_state_id, std::move(result));
        }
        ), cmd.change);

        co_await _sp.mutate_locally({convert_history_mutation(std::move(cmd.history_append), _sp.data_dictionary())}, nullptr);
    }
}

future<raft::snapshot_id> group0_state_machine::take_snapshot() {
    return make_ready_future<raft::snapshot_id>(raft::snapshot_id::create_random_id());
}

void group0_state_machine::drop_snapshot(raft::snapshot_id id) {
    (void) id;
}

future<> group0_state_machine::load_snapshot(raft::snapshot_id id) {
    return make_ready_future<>();
}

future<> group0_state_machine::transfer_snapshot(gms::inet_address from, raft::snapshot_descriptor snp) {
    // Note that this may bring newer state than the group0 state machine raft's
    // log, so some raft entries may be double applied, but since the state
    // machine is idempotent it is not a problem.

    slogger.trace("transfer snapshot from {} index {} snp id {}", from, snp.idx, snp.id);
    netw::messaging_service::msg_addr addr{from, 0};
    // (Ab)use MIGRATION_REQUEST to also transfer group0 history table mutation besides schema tables mutations.
    auto [_, cm] = co_await _mm._messaging.send_migration_request(addr, netw::schema_pull_options { .group0_snapshot_transfer = true });
    if (!cm) {
        // If we're running this code then remote supports Raft group 0, so it should also support canonical mutations
        // (which were introduced a long time ago).
        on_internal_error(slogger, "Expected MIGRATION_REQUEST to return canonical mutations");
    }
    auto history_mut = extract_history_mutation(*cm, _sp.data_dictionary());

    // TODO ensure atomicity of snapshot application in presence of crashes (see TODO in `apply`)

    auto read_apply_mutex_holder = co_await get_units(_client._read_apply_mutex, 1);

    co_await _mm.merge_schema_from(addr, std::move(*cm));

    co_await _sp.mutate_locally({std::move(history_mut)}, nullptr);
}

future<> group0_state_machine::abort() {
    return make_ready_future<>();
}

} // end of namespace service

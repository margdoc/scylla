/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <optional>
#include <seastar/core/coroutine.hh>
#include "raft_group0_client.hh"

#include "frozen_schema.hh"
#include "schema_mutations.hh"
#include "seastar/core/future.hh"
#include "serialization_visitors.hh"
#include "serializer.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/uuid.dist.hh"
#include "serializer_impl.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"
#include "idl/group0_tables_lang.dist.hh"
#include "idl/group0_tables_lang.dist.impl.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "service/raft/group0_state_machine.hh"
#include "utils/UUID.hh"


namespace service {

static logging::logger logger("group0_client");

/* *** Linearizing group 0 operations ***
 *
 * Group 0 changes (e.g. schema changes) are performed through Raft commands, which are executing in the same order
 * on every node, according to the order they appear in the Raft log
 * (executing a command happens in `group0_state_machine::apply`).
 * The commands contain mutations which modify tables that store group 0 state.
 *
 * However, constructing these mutations often requires reading the current state and validating the change against it.
 * This happens outside the code which applies the commands in order and may race with it. At the moment of applying
 * a command, the mutations stored within may be 'invalid' because a different command managed to be concurrently applied,
 * changing the state.
 *
 * For example, consider the sequence of commands:
 *
 * C1, C2, C3.
 *
 * Suppose that mutations inside C2 were constructed on a node which already applied C1. Thus, when applying C2,
 * the state of group 0 is the same as when the change was validated and its mutations were constructed.
 *
 * On the other hand, suppose that mutations inside C3 were also constructed on a node which applied C1, but didn't
 * apply C2 yet. This could easily happen e.g. when C2 and C3 were constructed concurrently on two different nodes.
 * Thus, when applying C3, the state of group 0 is different than it was when validating the change and constructing
 * its mutations: the state consists of the changes from C1 and C2, but when C3 was created, it used the state consisting
 * of changes from C1 (but not C2). Thus the mutations in C3 are not valid and we must not apply them.
 *
 * To protect ourselves from applying such 'obsolete' changes, we detect such commands during `group0_state_machine:apply`
 * and skip their mutations.
 *
 * For this, group 0 state was extended with a 'history table' (system.group0_history), which stores a sequence of
 * 'group 0 state IDs' (which are timeuuids). Each group 0 command also holds a unique state ID; if the command is successful,
 * the ID is appended to the history table. Each command also stores a 'previous state ID'; the change described by the command
 * is only applied when this 'previous state ID' is equal to the last state ID in the history table. If it's different,
 * we skip the change.
 *
 * To perform a group 0 change the user must first read the last state ID from the history table. This happens by obtaining
 * a `group0_guard` through `migration_manager::start_group0_operation`; the observed last state ID is stored in
 * `_observed_group0_state_id`. `start_group0_operation` also generates a new state ID for this change and stores it in
 * `_new_group0_state_id`. We ensure that the new state ID is greater than the observed state ID (in timeuuid order).
 *
 * The user then reads group 0 state, validates the change against the observed state, and constructs the mutations
 * which modify group 0 state. Finally, the user calls `announce`, passing the mutations and the guard.
 *
 * `announce` constructs a command for the group 0 state machine. The command stores the mutations and the state IDs.
 *
 * When the command is applied, we compare the stored observed state ID against the last state ID in the history table.
 * If it's the same, that means no change happened in between - no other command managed to 'sneak in' between the moment
 * the user started the operation and the moment the command was applied.
 *
 * The user must use `group0_guard::write_timestamp()` when constructing the mutations. The timestamp is extracted
 * from the new state ID. This ensures that mutations applied by successful commands have monotonic timestamps.
 * Indeed: the state IDs of successful commands are increasing (the previous state ID of a command that is successful
 * is equal to the new state ID of the previous successful command, and we ensure that the new state ID of a command
 * is greater than the previous state ID of this command).
 *
 * To perform a linearized group 0 read the user must also obtain a `group0_guard`. This ensures that all previously
 * completed changes are visible on this node, as obtaining the guard requires performing a Raft read barrier.
 *
 * Furthermore, obtaining the guard ensures that we don't read partial state, since it holds a lock that is also taken
 * during command application (`_read_apply_mutex_holder`). The lock is released just before sending the command to Raft.
 * TODO: we may still read partial state if we crash in the middle of command application.
 * See `group0_state_machine::apply` for a proposed fix.
 *
 * Obtaining the guard also ensures that there is no concurrent group 0 operation running on this node using another lock
 * (`_operation_mutex_holder`); if we allowed multiple concurrent operations to run, some of them could fail
 * due to the state ID protection. Concurrent operations may still run on different nodes. This lock is thus used
 * for improving liveness of operations running on the same node by serializing them.
 */
struct group0_guard::impl {
    semaphore_units<> _operation_mutex_holder;
    semaphore_units<> _read_apply_mutex_holder;

    utils::UUID _observed_group0_state_id;
    utils::UUID _new_group0_state_id;

    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;

    impl(semaphore_units<> operation_mutex_holder, semaphore_units<> read_apply_mutex_holder, utils::UUID observed_group0_state_id, utils::UUID new_group0_state_id)
        : _operation_mutex_holder(std::move(operation_mutex_holder)), _read_apply_mutex_holder(std::move(read_apply_mutex_holder)),
          _observed_group0_state_id(observed_group0_state_id), _new_group0_state_id(new_group0_state_id)
    {}

    void release_read_apply_mutex() {
        assert(_read_apply_mutex_holder.count() == 1);
        _read_apply_mutex_holder.return_units(1);
    }
};

group0_guard::group0_guard(std::unique_ptr<impl> p) : _impl(std::move(p)) {}

group0_guard::~group0_guard() = default;

group0_guard::group0_guard(group0_guard&&) noexcept = default;

utils::UUID group0_guard::observed_group0_state_id() const {
    return _impl->_observed_group0_state_id;
}

utils::UUID group0_guard::new_group0_state_id() const {
    return _impl->_new_group0_state_id;
}

api::timestamp_type group0_guard::write_timestamp() const {
    return utils::UUID_gen::micros_timestamp(_impl->_new_group0_state_id);
}


void raft_group0_client::set_history_gc_duration(gc_clock::duration d) {
    _history_gc_duration = d;
}

semaphore& raft_group0_client::operation_mutex() {
    return _operation_mutex;
}

static utils::UUID generate_group0_state_id(utils::UUID prev_state_id) {
    auto ts = api::new_timestamp();
    if (prev_state_id != utils::UUID{}) {
        auto lower_bound = utils::UUID_gen::micros_timestamp(prev_state_id);
        if (ts <= lower_bound) {
            ts = lower_bound + 1;
        }
    }
    return utils::UUID_gen::get_random_time_UUID_from_micros(std::chrono::microseconds{ts});
}

future<> raft_group0_client::add_entry(group0_command group0_cmd, group0_guard guard, seastar::abort_source* as) {
    if (this_shard_id() != 0) {
        // This should not happen since all places which construct `group0_guard` also check that they are on shard 0.
        // Note: `group0_guard::impl` is private to this module, making this easy to verify.
        on_internal_error(logger, "add_entry: must run on shard 0");
    }

    auto new_group0_state_id = guard.new_group0_state_id();

    co_await [&, guard = std::move(guard)] () -> future<> { // lambda is needed to limit guard's lifetime
        raft::command cmd;
        ser::serialize(cmd, group0_cmd);

        // Release the read_apply mutex so `group0_state_machine::apply` can take it.
        guard._impl->release_read_apply_mutex();

        bool retry;
        do {
            retry = false;
            try {
                co_await _raft_gr.group0().add_entry(cmd, raft::wait_type::applied, as);
            } catch (const raft::dropped_entry& e) {
                logger.warn("add_entry: returned \"{}\". Retrying the command (prev_state_id: {}, new_state_id: {})",
                        e, group0_cmd.prev_state_id, group0_cmd.new_state_id);
                retry = true;
            } catch (const raft::commit_status_unknown& e) {
                logger.warn("add_entry: returned \"{}\". Retrying the command (prev_state_id: {}, new_state_id: {})",
                        e, group0_cmd.prev_state_id, group0_cmd.new_state_id);
                retry = true;
            } catch (const raft::not_a_leader& e) {
                // This should not happen since follower-to-leader entry forwarding is enabled in group 0.
                // Just fail the operation by propagating the error.
                logger.error("add_entry: unexpected `not_a_leader` error: \"{}\". Please file an issue.", e);
                throw;
            }

            // Thanks to the `prev_state_id` check in `group0_state_machine::apply`, the command is idempotent.
            // It's safe to retry it, even if it means it will be applied multiple times; only the first time
            // can have an effect.
        } while (retry);

        // dropping the guard releases `_group0_operation_mutex`, allowing other operations
        // on this node to proceed
    } ();

    if (!(co_await db::system_keyspace::group0_history_contains(new_group0_state_id))) {
        // The command was applied but the history table does not contain the new group 0 state ID.
        // This means `apply` skipped the change due to previous state ID mismatch.
        throw group0_concurrent_modification{};
    }
}

future<> raft_group0_client::add_entry_unguarded(group0_command group0_cmd, seastar::abort_source* as) {
    if (this_shard_id() != 0) {
        // This should not happen since all places which construct `group0_guard` also check that they are on shard 0.
        // Note: `group0_guard::impl` is private to this module, making this easy to verify.
        on_internal_error(logger, "add_entry: must run on shard 0");
    }

    auto new_group0_state_id = generate_group0_state_id(group0_cmd.prev_state_id.value_or(utils::UUID{}));

    raft::command cmd;
    ser::serialize(cmd, group0_cmd);

    bool retry;
    do {
        retry = false;
        try {
            co_await _raft_gr.group0().add_entry(cmd, raft::wait_type::applied, as);
        } catch (const raft::dropped_entry& e) {
            logger.warn("add_entry: returned \"{}\". Retrying the command (prev_state_id: {}, new_state_id: {})",
                    e, group0_cmd.prev_state_id, group0_cmd.new_state_id);
            retry = true;
        } catch (const raft::commit_status_unknown& e) {
            logger.warn("add_entry: returned \"{}\". Retrying the command (prev_state_id: {}, new_state_id: {})",
                    e, group0_cmd.prev_state_id, group0_cmd.new_state_id);
            retry = true;
        } catch (const raft::not_a_leader& e) {
            // This should not happen since follower-to-leader entry forwarding is enabled in group 0.
            // Just fail the operation by propagating the error.
            logger.error("add_entry: unexpected `not_a_leader` error: \"{}\". Please file an issue.", e);
            throw;
        }

        // Thanks to the `prev_state_id` check in `group0_state_machine::apply`, the command is idempotent.
        // It's safe to retry it, even if it means it will be applied multiple times; only the first time
        // can have an effect.
    } while (retry);
}

future<group0_guard> raft_group0_client::start_operation(seastar::abort_source* as) {
    if (is_enabled()) {
        if (this_shard_id() != 0) {
            on_internal_error(logger, "start_group0_operation: must run on shard 0");
        }

        auto operation_holder = co_await get_units(_operation_mutex, 1);
        co_await _raft_gr.group0().read_barrier(as);

        // Take `_group0_read_apply_mutex` *after* read barrier.
        // Read barrier may wait for `group0_state_machine::apply` which also takes this mutex.
        auto read_apply_holder = co_await get_units(_read_apply_mutex, 1);

        auto observed_group0_state_id = co_await db::system_keyspace::get_last_group0_state_id();
        auto new_group0_state_id = generate_group0_state_id(observed_group0_state_id);

        co_return group0_guard {
            std::make_unique<group0_guard::impl>(
                std::move(operation_holder),
                std::move(read_apply_holder),
                observed_group0_state_id,
                new_group0_state_id
            )
        };
    }

    co_return group0_guard {
        std::make_unique<group0_guard::impl>(
            semaphore_units<>{},
            semaphore_units<>{},
            utils::UUID{},
            generate_group0_state_id(utils::UUID{})
        )
    };
}

group0_command raft_group0_client::prepare_command(schema_change change, group0_guard& guard, std::string_view description) {
    group0_command group0_cmd {
        .change{std::move(change)},
        .history_append{db::system_keyspace::make_group0_history_state_id_mutation(
            guard.new_group0_state_id(), _history_gc_duration, description)},

        // IMPORTANT: the retry mechanism below assumes that `prev_state_id` is engaged (not nullopt).
        // Here it is: the return type of `guard.observerd_group0_state_id()` is `utils::UUID`.
        .prev_state_id{guard.observed_group0_state_id()},
        .new_state_id{guard.new_group0_state_id()},

        .creator_addr{utils::fb_utilities::get_broadcast_address()},
        .creator_id{_raft_gr.group0().id()}
    };

    return group0_cmd;
}

group0_command raft_group0_client::prepare_command(table_query query) {
    const auto new_group0_state_id = generate_group0_state_id(utils::UUID{});

    group0_command group0_cmd {
        .change{std::move(query)},
        .history_append{db::system_keyspace::make_group0_history_state_id_mutation(
            new_group0_state_id, _history_gc_duration, "")},

        .prev_state_id{std::nullopt},
        .new_state_id{new_group0_state_id},

        .creator_addr{utils::fb_utilities::get_broadcast_address()},
        .creator_id{_raft_gr.group0().id()}
    };

    return group0_cmd;
}

void raft_group0_client::set_query_result(utils::UUID query_id, raft::group0_tables::query_result qr) {
    _results.emplace(query_id, std::move(qr));
}

raft::group0_tables::query_result raft_group0_client::get_query_result(utils::UUID query_id) {
    assert(_results.contains(query_id));

    return std::move(_results[query_id]);
}

void raft_group0_client::remove_query_result(utils::UUID query_id) {
    if (_results.contains(query_id)) {
        _results.erase(query_id);
    }
}

}

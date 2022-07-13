/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "lang.hh"

#include <exception>
#include <fmt/core.h>
#include <optional>
#include <variant>

#include <boost/range/adaptors.hpp>

#include "cql3/operation.hh"
#include "cql3/expr/expression.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/statements/statement_type.hh"
#include "cql3/statements/update_statement.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include "schema.hh"
#include "service/raft/raft_group0_client.hh"
#include "types.hh"
#include "utils/overloaded_functor.hh"


namespace raft::group0_tables {

using namespace std::string_literals;
using boost::adaptors::transformed;

// Helper function to check if statement's type is select and it selects an entire table.
static
bool is_select_all_statement(const cql3::cql_statement& statement) {
    auto s = dynamic_cast<const cql3::statements::primary_key_select_statement*>(&statement);

    if (!s) {
        return false;
    }

    const auto* conjunction = cql3::expr::as_if<cql3::expr::conjunction>(&s->get_restrictions()->get_partition_key_restrictions());

    return conjunction && conjunction->children.size() == 0;
}

bool is_group0_table_statement(const cql3::cql_statement& statement) {
    return statement.depends_on(db::system_keyspace::NAME, db::system_keyspace::GROUP0_KV_STORE) &&
            !is_select_all_statement(statement);
}


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

static
std::string get_key(const cql3::expr::expression& partition_key_restrictions) {
    const auto* conjunction = cql3::expr::as_if<cql3::expr::conjunction>(&partition_key_restrictions);

    if (!conjunction || conjunction->children.size() != 1) {
        throw unsupported_operation_error("partition key restriction: {}", partition_key_restrictions);
    }

    const auto* key_restriction = cql3::expr::as_if<cql3::expr::binary_operator>(&conjunction->children[0]);

    if (!key_restriction) {
        throw unsupported_operation_error("partition key restriction: {}", *conjunction);
    }

    const auto* column = cql3::expr::as_if<cql3::expr::column_value>(&key_restriction->lhs);
    const auto* value = cql3::expr::as_if<cql3::expr::constant>(&key_restriction->rhs);

    if (!column || column->col->kind != column_kind::partition_key ||
        !value || key_restriction->op != cql3::expr::oper_t::EQ) {
        throw unsupported_operation_error("key restriction: {}", *key_restriction);
    }

    return value->view().deserialize<std::string>(*utf8_type);
}

static
bool is_selecting_only_value(const cql3::statements::primary_key_select_statement& statement) {
    const auto selection = statement.get_selection();

    return selection->is_trivial() &&
           selection->get_column_count() == 1 &&
           selection->get_columns()[0]->name() == "value";
}

static
std::string get_new_value(const cql3::statements::modification_statement& statement) {
    if (statement.get_column_operations().size() != 1) {
        throw unsupported_operation_error("modifications: {}", fmt::join(
            statement.get_column_operations() | transformed([] (const auto& op) { return fmt::format("{}", op->get_expression()); }), ", "));
    }

    const auto optional_expression = statement.get_column_operations()[0]->get_expression();

    if (!optional_expression) {
        throw unsupported_operation_error("modifications: {}", fmt::join(
            statement.get_column_operations() | transformed([] (const auto& op) { return fmt::format("{}", op->get_expression()); }), ", "));
    }


    const auto* value = cql3::expr::as_if<cql3::expr::constant>(&*optional_expression);

    if (!value) {
        throw unsupported_operation_error("modification: {}", optional_expression);
    }

    return value->view().deserialize<std::string>(*utf8_type);
}

static
std::optional<std::string> get_value_condition(const cql3::statements::modification_statement& statement) {
    if (statement.get_regular_conditions().size() == 0) {
        return std::nullopt;
    }

    if (statement.get_regular_conditions().size() > 1) {
        throw unsupported_operation_error("conditions: {}", fmt::join(
            statement.get_regular_conditions() | transformed([] (const auto& cond) {
                return fmt::format("{}{}", cond->get_operation(), cond->get_value());
            }), ", "));
    }

    const auto& condition = statement.get_regular_conditions()[0];

    if (!condition->get_value() || condition->get_operation() != cql3::expr::oper_t::EQ) {
        throw unsupported_operation_error("condition: {}{}", condition->get_operation(), condition->get_value());
    }

    const auto* value = cql3::expr::as_if<cql3::expr::constant>(&*condition->get_value());

    if (!value) {
        throw unsupported_operation_error("condition: {}{}", condition->get_operation(), condition->get_value());
    }

    return value->view().deserialize<std::string>(*utf8_type);
}

static
query compile(const cql3::cql_statement& statement) {
    if (auto s = dynamic_cast<const cql3::statements::primary_key_select_statement*>(&statement)) {
        if (!is_selecting_only_value(*s)) {
            throw unsupported_operation_error("only 'value' selector is allowed");
        }

        return query { select_query {
                .key = get_key(s->get_restrictions()->get_partition_key_restrictions())
            } };
    } else if (auto s = dynamic_cast<const cql3::statements::modification_statement*>(&statement)) {
        return query { update_query {
                .key = get_key(s->restrictions().get_partition_key_restrictions()),
                .new_value = get_new_value(*s),
                .value_condition = get_value_condition(*s),
            } };
    }

    throw unsupported_operation_error();
}

future<::shared_ptr<cql_transport::messages::result_message>> execute(service::raft_group0_client& group0_client, const cql3::cql_statement& statement) {
    compile(statement);
    throw exceptions::invalid_request_exception{"executing queries on group0_kv_store is currently not implemented"};
}

}
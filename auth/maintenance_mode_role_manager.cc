/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/maintenance_mode_role_manager.hh"

#include <seastar/core/future.hh>
#include <stdexcept>
#include "log.hh"
#include "utils/class_registrator.hh"

namespace auth {

constexpr std::string_view maintenance_mode_role_manager_name = "com.scylladb.auth.MaintenanceModeRoleManager";

static const class_registrator<
        role_manager,
        maintenance_mode_role_manager,
        cql3::query_processor&,
        ::service::migration_manager&> registration(sstring{maintenance_mode_role_manager_name});


std::string_view maintenance_mode_role_manager::qualified_java_name() const noexcept {
        return maintenance_mode_role_manager_name;
}

const resource_set& maintenance_mode_role_manager::protected_resources() const {
        static const resource_set resources{};

        return resources;
}

future<> maintenance_mode_role_manager::start() {
        return make_ready_future<>();
}

future<> maintenance_mode_role_manager::stop() {
        return make_ready_future<>();
}

future<> maintenance_mode_role_manager::create(std::string_view role_name, const role_config&) {
        return make_exception_future<>(
        std::runtime_error("CREATE operation is not supported by MaintenanceModeRoleManager"));
}

future<> maintenance_mode_role_manager::drop(std::string_view role_name) {
        return make_exception_future<>(
                std::runtime_error("DROP operation is not supported by MaintenanceModeRoleManager"));
}

future<> maintenance_mode_role_manager::alter(std::string_view role_name, const role_config_update&) {
        return make_exception_future<>(
                std::runtime_error("ALTER operation is not supported by MaintenanceModeRoleManager"));
}

future<> maintenance_mode_role_manager::grant(std::string_view grantee_name, std::string_view role_name) {
        return make_exception_future<>(
                std::runtime_error("GRANT operation is not supported by MaintenanceModeRoleManager"));
}

future<> maintenance_mode_role_manager::revoke(std::string_view revokee_name, std::string_view role_name) {
        return make_exception_future<>(
                std::runtime_error("REVOKE operation is not supported by MaintenanceModeRoleManager"));
}

future<role_set> maintenance_mode_role_manager::query_granted(std::string_view grantee_name, recursive_role_query) {
        return make_exception_future<role_set>(
                std::runtime_error("QUERY GRANTED operation is not supported by MaintenanceModeRoleManager"));
}

future<role_set> maintenance_mode_role_manager::query_all() {
        return make_exception_future<role_set>(
                std::runtime_error("QUERY ALL operation is not supported by MaintenanceModeRoleManager"));
}

future<bool> maintenance_mode_role_manager::exists(std::string_view role_name) {
        return make_exception_future<bool>(
                std::runtime_error("EXISTS operation is not supported by MaintenanceModeRoleManager"));
}

future<bool> maintenance_mode_role_manager::is_superuser(std::string_view role_name) {
        return make_ready_future<bool>(true);
}

future<bool> maintenance_mode_role_manager::can_login(std::string_view role_name) {
        return make_ready_future<bool>(true);
}

future<std::optional<sstring>> maintenance_mode_role_manager::get_attribute(std::string_view role_name, std::string_view attribute_name) {
        return make_exception_future<std::optional<sstring>>(
                std::runtime_error("GET ATTRIBUTE operation is not supported by MaintenanceModeRoleManager"));
}

future<role_manager::attribute_vals> maintenance_mode_role_manager::query_attribute_for_all(std::string_view attribute_name) {
        return make_exception_future<role_manager::attribute_vals>(
                std::runtime_error("QUERY ATTRIBUTE operation is not supported by MaintenanceModeRoleManager"));
}

future<> maintenance_mode_role_manager::set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value) {
        return make_exception_future<>(
                std::runtime_error("SET ATTRIBUTE operation is not supported by MaintenanceModeRoleManager"));
}

future<> maintenance_mode_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name) {
        return make_exception_future<>(
                std::runtime_error("REMOVE ATTRIBUTE operation is not supported by MaintenanceModeRoleManager"));
}

}

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import pytest

from test.pylib.log_browsing import open_log_file
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_generation_service_shutdown(manager: ManagerClient, thread_pool: ThreadPoolExecutor) -> None:
    """
    Tests if shutdown process waits for applying group0 commands before stopping the generation service.
    It forces to wait for generation service to stop before using `handle_cdc_generation` method.
    """
    s1 = await manager.server_add()

    log_file = open_log_file(thread_pool, s1.ip_addr)

    marker = log_file.mark()
    # Wait before calling `cdc::generation_service::handle_cdc_generation`.
    generation_handler = await inject_error_one_shot(manager.api, s1.ip_addr, 'handle_cdc_generation::wait')

    async def start_second_node():
        # It is used to force reloading the topology state on the first node.
        with pytest.raises(asyncio.CancelledError):
            # Adding a second node should never finish, because the first node will be stopped.
            await manager.server_add()

    task = asyncio.create_task(start_second_node())

    async def stop_first_node():
        await inject_error_one_shot(manager.api, s1.ip_addr, 'stop_cdc_generation_service::wait')

        await log_file.wait_for("topology_state_load: before handle_cdc_generation", from_mark=marker)
        # First node waits just before calling `cdc::generation_service::handle_cdc_generation`.

        async def stop_second_node():
            await manager.server_stop_gracefully(s1.server_id)

        async def proceed_with_handle_cdc_generation():
            # Wait for the shutdown of the generation service.
            await log_file.wait_for("stop_cdc_generation_service wait")
            # Proceed with `handle_cdc_generation`.

            # If the code is correct, this will never happen, since the shutdown process will wait for
            # applying group0 commands before stopping the group0 service which stops before the generation service.

            # If the shutdown process doesn't wait for applying group0 commands, this will fail with SEGFAULT,
            # because the generation service will be stopped.
            await generation_handler.message()

        await asyncio.gather(stop_second_node(), proceed_with_handle_cdc_generation())
        task.cancel()

    await asyncio.gather(task, stop_first_node())

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.topology.util import check_token_ring_and_group0_consistency

import pytest

@pytest.mark.asyncio
async def test_send_data_in_parts(manager: ManagerClient):
    first_server = await manager.server_add()

    async with inject_error(manager.api, first_server.ip_addr, 'cdc_generation_mutations_overestimate'):
        # cdc generation data should be sent in parts
        for _ in range(2):
            await manager.server_add()

    # cdc generation data should be sent in one piece
    await manager.server_add()

    await check_token_ring_and_group0_consistency(manager)
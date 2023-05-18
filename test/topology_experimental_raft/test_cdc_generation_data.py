from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.topology.util import check_token_ring_and_group0_consistency

import pytest

@pytest.mark.asyncio
async def test_send_data_in_parts(manager: ManagerClient):
    first_server = await manager.server_add(config={
        'commitlog_segment_size_in_mb': 2
    })

    async with inject_error(manager.api, first_server.ip_addr, 'cdc_generation_mutations_overestimate'):
        for _ in range(3):
            await manager.server_add(config={
                'commitlog_segment_size_in_mb': 2
            })

    await check_token_ring_and_group0_consistency(manager)
import logging
import time
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import read_barrier, wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_generation_service_shutdown(manager: ManagerClient) -> None:
    await manager.server_add()

    try:
        await manager.server_add(config={
            'error_injections_at_startup': ['stop_after_joining_group0', 'handle_cdc_generation::sleep', 'stop_raft::sleep']
        })
    except:
        # Node stops before it advertised itself in gossip, so manager.server_add throws an exception
        pass
    else:
        assert False, "Node should stop before it advertised itself in gossip"

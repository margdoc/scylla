import pytest
import logging
import asyncio

from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_destination_id(manager: ManagerClient, random_tables: RandomTables) -> None:
    stopped = False
    ddl_failed = False

    async def do_ddl():
        nonlocal ddl_failed
        iteration = 0
        while not stopped:
            logger.debug(f'>>>>> ddl, iteration {iteration} started')
            try:
                await random_tables.add_tables(5, 5, if_not_exists=True)
                await random_tables.verify_schema()
                while len(random_tables.tables) > 0:
                    await random_tables.drop_table(random_tables.tables[-1])
                logger.debug(f'>>>>> ddl, iteration {iteration} finished')
            except:
                logger.exception(f'>>>>> ddl, iteration {iteration} failed')
                ddl_failed = True
                raise
            iteration += 1

    servers = await manager.running_servers()
    ddl_task = asyncio.create_task(do_ddl())
    try:
        logger.debug(">>>>> server replacing started")
        await manager.server_stop(servers[2].server_id)
        replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = True, use_host_id = True)
        await manager.server_add(replace_cfg)
    finally:
        logger.debug(">>>>> server replacing finished, waiting for ddl fiber")
        stopped = True
        await ddl_task
        logger.debug(">>>>> ddl fiber done, finished")
    assert not ddl_failed
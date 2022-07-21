#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import random
import string
from typing import Dict, List, Optional
import pytest


def random_string(size=10) -> str:
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(size))

def random_bool() -> bool:
    return bool(random.getrandbits(1))



async def select(cql, key: str) -> Optional[str]:
    result = list(await cql.run_async(f"SELECT value FROM system.group0_kv_store WHERE key = '{key}'"))

    if len(result) == 0:
        return None

    assert len(result) == 1
    return result[0].value

async def update(cql, key: str, new_value: str) -> str:
    await cql.run_async(f"UPDATE system.group0_kv_store SET value = '{new_value}' WHERE key = '{key}';")

async def update_conditional(cql, key: str, new_value: str, value_condition: str) -> str:
    await cql.run_async(f"UPDATE system.group0_kv_store SET value = '{new_value}' WHERE key = '{key}' IF value = '{value_condition}';")


@pytest.mark.asyncio
async def test_group0_kv_store(cql):
    experimental_features = list(await cql.run_async("SELECT value FROM system.config WHERE name = 'experimental_features'"))[0].value
    assert "group0-tables" in experimental_features

    random_strings: List[str] = [random_string() for _ in range(42)]
    kv_store: Dict[str, str] = {}
    last_key: str = ""
    last_value: str = ""

    for _ in range(2137):
        key = random.choice(random_strings)

        query_type = random.randint(0, 3)
        if query_type == 0:
            value = await select(cql, key)
            assert (value is None and key not in kv_store) or value == kv_store[key]
        else:
            new_value = random.choice(random_strings)
            if query_type == 1:
                await update(cql, key, new_value)
                last_value = kv_store[key] = new_value
            else:
                key = last_key
                value_condition = last_value if random_bool() else random.choice(random_strings)
                await update_conditional(cql, key, new_value, value_condition)
                if key in kv_store and kv_store[key] == value_condition:
                    last_value = kv_store[key] = new_value
        last_key = key

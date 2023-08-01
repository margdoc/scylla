# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Optional
import pytest
import os
import socket
import re

from test.pylib.internal_types import IPAddress


logger = logging.getLogger(__name__)

 # Used for asynchronous IO operations
@pytest.fixture(scope="session")
def thread_pool() -> ThreadPoolExecutor:
    return ThreadPoolExecutor()

# Class for browsing a Scylla log file.
# Based on scylla-ccm implementation of log browsing.
class ScyllaLogFile():
    def __init__(self, thread_pool: ThreadPoolExecutor, logfile_path: str):
        self.thread_pool = thread_pool # used for asynchronous IO operations
        self.file = logfile_path

    # Returns "a mark" to the current position of this node Scylla log.
    # This is for use with the from_mark parameter of watch_log_for method,
    # allowing to watch the log from the position when this method was called.
    def mark(self) -> int:
        with open(self.file, 'r') as f:
            f.seek(0, os.SEEK_END)
            return f.tell()

    # wait_for_log() checks if the log contains the given message.
    # Because it may take time for the log message to be flushed, and sometimes
    # we may want to look for messages about various delayed events, this
    # function doesn't give up when it reaches the end of file, and rather
    # retries until a given timeout. The timeout may be long, because successful
    # tests will not wait for it. Note, however, that long timeouts will make
    # xfailing tests slow.
    # The timeout is in seconds.
    # If from_mark is given, the log is searched from that position, otherwise
    # from the beginning.
    async def wait_for(self, pattern: str | re.Pattern, from_mark: Optional[int] = None, timeout: int = 600) -> None:
        prog = re.compile(pattern)
        with open(self.file, 'r') as f:
            if from_mark is not None:
                f.seek(from_mark)

            loop = asyncio.get_running_loop()

            async with asyncio.timeout(timeout):
                logger.debug("Waiting for log message: %s", pattern)
                while True:
                    line = await loop.run_in_executor(self.thread_pool, f.readline)
                    if line and prog.search(line):
                        logger.debug("Found log message: %s", line)
                        return


    # Returns a list of lines matching the regular expression in the Scylla log.
    # The list contains tuples of (line, match), where line is the full line
    # from the log file, and match is the re.Match object for the matching
    # expression.
    # If filter_expr is given, only lines which do not match it are returned.
    # If from_mark is given, the log is searched from that position, otherwise
    # from the beginning.
    def grep(self, expr: str | re.Pattern, filter_expr: Optional[str | re.Pattern] = None,
             from_mark: Optional[int] = None) -> list[(str, re.Match[str])]:
        matchings = []
        pattern = re.compile(expr)
        if filter_expr:
            filter_pattern = re.compile(filter_expr)
        else:
            filter_pattern = None
        with open(self.file) as f:
            if from_mark:
                f.seek(from_mark)
            for line in f:
                m = pattern.search(line)
                if m and not (filter_pattern and re.search(filter_pattern, line)):
                    matchings.append((line, m))
        return matchings

# Utility function for trying to find a local process which is listening to
# the address and port to which our our CQL connection is connected. If such a
# process exists, return its process id (as a string). Otherwise, return None.
# Note that the local process needs to belong to the same user running this
# test, or it cannot be found.
def local_process_id(server_ip: IPAddress, port: int = 9042) -> Optional[int]:
    ip = socket.gethostbyname(server_ip)
    # Implement something like the shell "lsof -Pni @{ip}:{port}", just
    # using /proc without any external shell command.
    # First, we look in /proc/net/tcp for a LISTEN socket (state 0x0A) at the
    # desired local address. The address is specially-formatted hex of the ip
    # and port, with 0100007F:2352 for 127.0.0.1:9042. We check for two
    # listening addresses: one is the specific IP address given, and the
    # other is listening on address 0 (INADDR_ANY).
    ip2hex = lambda ip: ''.join([f'{int(x):02X}' for x in reversed(ip.split('.'))])
    port2hex = lambda port: f'{int(port):04X}'
    addr1 = ip2hex(ip) + ':' + port2hex(port)
    addr2 = ip2hex('0.0.0.0') + ':' + port2hex(port)
    LISTEN = '0A'
    with open('/proc/net/tcp', 'r') as f:
        for line in f:
            cols = line.split()
            if cols[3] == LISTEN and (cols[1] == addr1 or cols[1] == addr2):
                inode = cols[9]
                break
        else:
            # Didn't find a process listening on the given address
            return None
    # Now look in /proc/*/fd/* for processes that have this socket "inode"
    # as one of its open files. We can only find a process that belongs to
    # the same user.
    target = f'socket:[{inode}]'
    for proc in os.listdir('/proc'):
        if not proc.isnumeric():
            continue
        dir = f'/proc/{proc}/fd/'
        try:
            for fd in os.listdir(dir):
                if os.readlink(dir + fd) == target:
                    # Found the process!
                    return proc
        except:
            # Ignore errors. We can't check processes we don't own.
            pass
    return None

# If the log file cannot be found, or it's not Scylla, this function calls
# pytest.skip() to skip any test which uses it.
# We look for the log file by looking for a local process listening to the
# given CQL connection, assuming its standard output is the log file, and
# then verifying that this file is a proper Scylla log file.
def logfile_path(pid: int) -> str:
    # Use /proc to find if its standard output is redirected to a file.
    # If it is, that's the log file. If it isn't a file, we don't where
    # the user is piping the log.
    try:
        log = os.readlink(f'/proc/{pid}/fd/1')
    except:
        pytest.skip("Can't find local log file")
    # If the process's standard output is some pipe or device, it's
    # not the log file we were hoping for...
    if not log.startswith('/') or not os.path.isfile(log):
        pytest.skip("Can't find local log file")
    # Scylla can be configured to put the log in syslog, not in the standard
    # output. So let's verify that the file which we found actually looks
    # like a Scylla log and isn't just empty or something... The Scylla log
    # file always starts with the line: "Scylla version ... starting ..."
    with open(log, 'r') as f:
        head = f.read(7)
        if head != 'Scylla ':
            pytest.skip("Not a Scylla log file")
        return log

def open_log_file(thread_pool: ThreadPoolExecutor, server_ip: IPAddress, port: int = 9042) -> ScyllaLogFile:
    pid = local_process_id(server_ip, port)
    if pid is None:
        pytest.skip("Can't find local log file")

    return ScyllaLogFile(thread_pool, logfile_path(pid))

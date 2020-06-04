# -*- coding: utf-8 -*-
# Usage: py.test tests

import os
import pytest
import subprocess
import redis
import random
import socket
import time


@pytest.fixture(scope="session", autouse=True)
def keyvi_server(request):

    def wait_for_connection(port):
        start_time = time.time()
        timeout = 3
        while time.time() < start_time + 3:
            try:
                sock = socket.socket()
                sock.connect(('localhost', port))
                return True
            except (socket.error, socket.timeout):
                timeout
                time.sleep(0.1)
            finally:
                # close socket manually for sake of PyPy
                sock.close()
        raise Exception("failed to start keyviserver")

    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "..", "build", "keyviserver")

    print("starting from: " + path)
    retry = 0
    while retry < 10:
        port = random.randint(10000, 20000)
        try:
            proc = subprocess.Popen([path, "-r", "-p", str(port)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            request.addfinalizer(proc.kill)
            wait_for_connection(port)
            return port
        except:pass
        retry += 1


def test_set_and_get(keyvi_server):
    r = redis.Redis(host='localhost', port=keyvi_server, db=0)
    r.set("a", "1")
    r.save()

    assert r.get("a") == b"1"


def test_mset(keyvi_server):
    r = redis.Redis(host='localhost', port=keyvi_server, db=0)
    r.mset({"a" : "1", "b" : "2"})
    r.save()

    assert r.get("a") == b"1"
    assert r.get("b") == b"2"


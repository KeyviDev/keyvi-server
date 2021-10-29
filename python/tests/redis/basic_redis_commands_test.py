# -*- coding: utf-8 -*-
# Usage: py.test tests

import json
import os
import pytest
import subprocess
import redis
import random
import socket
import time


@pytest.fixture(scope="module", autouse=True)
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


def test_crud(keyvi_server):
    r = redis.Redis(host='localhost', port=keyvi_server, db=0)
    r.set("a", json.dumps({"a":42}))
    r.save()
    assert r.exists("a")
    v = r.get("a")
    assert v is not None
    assert json.loads(v) == {"a": 42}
    r.set("a", json.dumps({"a":42, "b":2}))
    r.save()
    v = r.get("a")
    assert v is not None
    assert json.loads(v) == {"a": 42, "b":2}
    r.delete("a")
    r.save()
    assert r.exists("a") == False
    v = r.get("a")
    assert v is None


def test_delete(keyvi_server):
    r = redis.Redis(host='localhost', port=keyvi_server, db=0)
    r.mset({"a":json.dumps({"x":22, "y": 34}), "b":json.dumps({"x":99, "y": 32}), "c":json.dumps({"x":12, "y": 54}), "d":json.dumps({"x":21, "y": 66})})
    r.save()
    assert r.exists("a", "b", "c", "d") == 4
    assert r.exists("a", "b", "c", "d", "e", "f") == 4
    assert r.exists("a", "b", "d", "e", "f") == 3
    assert r.exists("d") == 1
    assert r.delete("a", "b") == 2
    r.save()
    assert r.exists("a", "b", "c", "d") == 2
    # delete always returns the number of keys, as it works async
    assert r.delete("a", "b") == 2
    assert r.delete("a", "b", "c", "d") == 4
    r.save()
    assert r.exists("a", "b", "c", "d") == 0


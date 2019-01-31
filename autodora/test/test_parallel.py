import os
import time
from multiprocessing import Process, Queue, Manager
from subprocess import check_output

import pytest

from autodora.parallel import run_function


def worker2(count):
    check_output("for i in `seq 1 {count}`; do echo $i; done".format(count=count), shell=True)


def worker(n1, n2, queue):
    print(n1, n2)
    process = Process(target=worker2, args=(n2,))
    process.start()
    pid = process.pid
    queue.put(pid)
    process.join()

    print("done")
    return n1


def simple_worker(n1, n2):
    result = 0
    for i in range(n2):
        result += 1 / (result + 1)
    print(result)
    return n1


@pytest.mark.skip(reason="Cannot currently kill child processes")
def test_parallel_child_processes():
    m = Manager()
    queue = m.Queue()
    timeout = 2
    start_time = time.time()
    run_function(worker, 1, 10000000, queue, timeout=timeout)
    duration = time.time() - start_time
    pid = queue.get()
    try:
        os.kill(pid, 0)
    except OSError:
        assert True
    else:
        assert False

    assert duration == pytest.approx(timeout, abs=1)

    start_time = time.time()
    timeout = 20
    run_function(worker, 1, 10, timeout=timeout)
    duration = time.time() - start_time
    assert duration < timeout


def test_parallel_simple_worker():
    timeout = 2
    start_time = time.time()
    run_function(simple_worker, 1, 10000000, timeout=timeout)
    duration = time.time() - start_time

    assert duration == pytest.approx(timeout, abs=1)

    start_time = time.time()
    timeout = 20
    run_function(simple_worker, 1, 10, timeout=timeout)
    duration = time.time() - start_time
    assert duration < timeout

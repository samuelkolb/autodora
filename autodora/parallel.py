import errno
import os
import signal
import subprocess
from multiprocessing import Queue, Manager
from multiprocessing.pool import Pool
from subprocess import TimeoutExpired
from traceback import print_exc
from typing import Optional, Union, Any

from .observe import Observer, dispatch


class Update:
    SENTINEL = "sentinel"
    STARTED = "started"
    DONE = "done"
    TIMEOUT = "timeout"
    FAILED = "failed"

    def __init__(self, status, index, command, meta):
        self.status = status
        self.index = index
        self.command = command
        self.meta = meta


class ParallelObserver(Observer):
    @dispatch
    def observe(self, update):
        # type: (Update) -> None
        raise NotImplementedError()


def observe(observer, queue, count=None):
    # type: (ParallelObserver, Queue, Optional[int]) -> None
    to_see = None if count is None else set(range(count))
    while to_see is None or len(to_see) > 0:
        update = queue.get()  # type: Union[Update, str]
        if update == Update.SENTINEL:
            return
        elif isinstance(update, Update):
            if update.status == Update.DONE or update.status == Update.TIMEOUT or update.status == Update.FAILED:
                if to_see:
                    to_see.remove(update.index)
            try:
                observer.observe(update)
            except Exception:
                print_exc()
        else:
            raise ValueError("Invalid update {}".format(update))


def run_command(args):
    i, meta, command, timeout, queue = args  # type: (int, Any, str, int, Queue)
    # TODO Capture output?
    with subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          preexec_fn=os.setsid) as process:
        try:
            if queue:
                queue.put(Update(Update.STARTED, i, command, meta))
            process.communicate(timeout=timeout)
            if queue:
                queue.put(Update(Update.DONE, i, command, meta))
        except TimeoutExpired:
            if queue:
                queue.put(Update(Update.TIMEOUT, i, command, meta))

            try:
                os.killpg(process.pid, signal.SIGINT)  # send signal to the process group
            except OSError as e:
                if e.errno != errno.ESRCH:
                    if e.errno == errno.EPERM:
                        os.waitpid(-process.pid, 0)
                else:
                    raise e

            process.communicate()


def run_commands(commands, processes=None, timeout=None, meta=None, observer=None):
    pool = Pool(processes=processes)
    manager, queue = None, None
    if observer:
        manager = Manager()
        queue = manager.Queue()

    if meta:
        commands = [(i, meta, command, timeout, queue) for i, (command, meta) in enumerate(zip(commands, meta))]
    else:
        commands = [(i, meta, command, timeout, queue) for i, command in enumerate(commands)]

    r = pool.map_async(run_command, commands)

    if observer:
        observe(observer, queue, len(commands))

    r.wait()

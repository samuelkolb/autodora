import os
import signal
import subprocess
from multiprocessing import Queue, Process
from multiprocessing.pool import Pool
from subprocess import TimeoutExpired
from typing import Optional, Union, Any

from .observe import Observer, dispatch


class Update:
    SENTINEL = "sentinel"
    STARTED = "started"
    DONE = "done"
    TIMEOUT = "timeout"

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
            if update.status == Update.DONE or update.status == Update.TIMEOUT:
                if to_see:
                    to_see.remove(update.index)
            observer.observe(update)
        else:
            raise ValueError("Invalid update {}".format(update))


def run_command(args):
    i, meta, command, timeout, queue = args  # type: (int, Any, str, int, Queue)
    with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, preexec_fn=os.setsid) as process:
        try:
            if queue:
                queue.put(Update(Update.STARTED, i, command, meta))
            process.communicate(timeout=timeout)
            if queue:
                queue.put(Update(Update.DONE, i, command, meta))
        except TimeoutExpired:
            if queue:
                queue.put(Update(Update.TIMEOUT, i, command, meta))
            os.killpg(process.pid, signal.SIGINT)  # send signal to the process group
            process.communicate()


def run_commands(commands, processes=None, timeout=None, meta=None, observer=None):
    pool = Pool(processes=processes)
    queue = Queue() if observer else None
    if meta:
        commands = [(i, meta, command, timeout, queue) for i, (command, meta) in enumerate(zip(commands, meta))]
    else:
        commands = [(i, meta, command, timeout, queue) for i, command in enumerate(commands)]

    observer_process = None
    if observer:
        observer_process = Process(target=observe, args=(observer, queue, len(commands)))
        observer_process.daemon = True
        observer_process.start()

    pool.map(run_command, commands)

    if observer_process:
        observer_process.join()

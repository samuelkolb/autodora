import atexit
import errno
import multiprocessing
import os
import signal
import subprocess
from multiprocessing import Queue, Manager, Process
from multiprocessing.pool import Pool
from subprocess import TimeoutExpired
from traceback import print_exc
from typing import Optional, Union, Any

from temporary import temp_file

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


def monitor(filename, monitor_queue):
    with open(filename, "w") as ref:
        while True:
            update = monitor_queue.get()  # type: Union[Update, str]
            if update == Update.SENTINEL:
                return
            elif update.status == Update.STARTED:
                print("ADD", update.meta, sep=" ", file=ref)
            else:
                print("REM", update.meta, sep=" ", file=ref)


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


def run_command(command, timeout=None):
    return worker((-1, None, command, timeout, None, None))


def run_function(f, *args, timeout=None, **kwargs):
    worker((-1, None, (f, args, kwargs), timeout, None, None))


def worker(args):
    i, meta, command, timeout, queue, m_queue = args  # type: (int, Any, Any, int, Queue, Queue)
    # TODO Capture output?

    if isinstance(command, str):
        is_string = True
    else:
        try:
            is_string = len(command) > 0 and not callable(command[0])
        except TypeError:
            raise ValueError("Command must be either string, args or function-args pair")

    if is_string:
        with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              start_new_session=True) as process:
            try:
                if m_queue:
                    m_queue.put(Update(Update.STARTED, i, command, process.pid))
                if queue:
                    queue.put(Update(Update.STARTED, i, command, meta))
                out, err = process.communicate(timeout=timeout)
                if queue:
                    if process.returncode == 0:
                        queue.put(Update(Update.DONE, i, command, meta))
                    else:
                        queue.put(Update(Update.FAILED, i, command, meta))
                return out.decode(), err.decode()
            except TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGTERM)  # send signal to the process group
                except OSError as e:
                    if e.errno != errno.ESRCH:
                        if e.errno == errno.EPERM:
                            os.waitpid(-process.pid, 0)
                    else:
                        raise e
                finally:
                    if queue:
                        queue.put(Update(Update.TIMEOUT, i, command, meta))

                process.communicate()
            finally:
                if m_queue:
                    m_queue.put(Update(Update.DONE, i, command, process.pid))

    else:
        assert isinstance(command, (tuple, list))
        if len(command) < 2:
            command = command + ([],)
        if len(command) < 3:
            command = command + (dict(),)

        f, args, kwargs = command

        p = multiprocessing.Process(target=f, args=args, kwargs=kwargs)
        if queue:
            queue.put(Update(Update.STARTED, i, command, meta))

        p.start()
        p.join(timeout)

        if p.is_alive():
            p.terminate()
            p.join()
            if queue:
                queue.put(Update(Update.TIMEOUT, i, command, meta))

        else:
            if queue:
                queue.put(Update(Update.DONE, i, command, meta))


def status(s):
    """Prints things in bold."""
    print('\033[1m{0}\033[0m'.format(s))


def run_commands(commands, processes=None, timeout=None, meta=None, observer=None):
    pool = Pool(processes=processes)
    manager, queue, m = None, None, None
    manager = Manager()
    m = manager.Queue()
    if observer:
        queue = manager.Queue()

    if meta:
        commands = [(i, meta, command, timeout, queue, m) for i, (command, meta) in enumerate(zip(commands, meta))]
    else:
        commands = [(i, meta, command, timeout, queue, m) for i, command in enumerate(commands)]

    with temp_file() as f:
        filename = str(f)

    m_process = Process(target=monitor, args=(filename, m))
    m_process.daemon = True
    m_process.start()

    def clean_exit():
        status("Keyboard interrupt intercepted, shutting down")
        try:
            m_process.terminate()
            m_process.join()
        except Exception:
            status("Monitor process could not be shut down")
            print_exc()

        try:
            pool.terminate()
            pool.join()
        except Exception:
            status("Pool could not be shut down")
            print_exc()

        status("Shutting down potential orphan processes")
        active = set()
        with open(filename) as ref:
            for line in ref:
                parts = line.split(" ")
                if parts[0] == "ADD":
                    active.add(int(parts[1]))
                elif parts[0] == "REM":
                    active.remove(int(parts[1]))

        for pid in active:
            try:
                print("Killing", pid)
                os.killpg(pid, signal.SIGTERM)  # send signal to the process group
            except OSError as e:
                if e.errno != errno.ESRCH:
                    if e.errno == errno.EPERM:
                        os.waitpid(-pid, 0)
                else:
                    raise e
            except Exception:
                print_exc()
                pass

        os.unlink(filename)

        status("Completely shut down")

    r = pool.map_async(worker, commands)
    atexit.register(clean_exit)

    if observer:
        observe(observer, queue, len(commands))

    r.wait()
    status("### DONE ##")
    m.put(Update.SENTINEL)
    m_process.join()


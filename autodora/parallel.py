import os
import signal
import subprocess
from multiprocessing.pool import Pool
from subprocess import TimeoutExpired


def run_command(args):
    command, timeout = args
    with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, preexec_fn=os.setsid) as process:
        try:
            process.communicate(timeout=timeout)
        except TimeoutExpired:
            os.killpg(process.pid, signal.SIGINT)  # send signal to the process group
            process.communicate()


def run_commands(commands, processes=None, timeout=None):
    pool = Pool(processes=processes)
    commands = [(command, timeout) for command in commands]
    pool.map(run_command, commands)

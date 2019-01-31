import inspect
import platform as platform_library
from datetime import datetime
from typing import TYPE_CHECKING, Type

from pebble import ProcessPool

from .observe import ProgressObserver
from .parallel import ParallelObserver, Update
from . import parallel
from .storage import export_storage
from concurrent.futures import TimeoutError as OtherTimeoutError


class ParallelToProcess(ParallelObserver):
    def __init__(self, observer, runner):
        # type: (ProgressObserver, CommandLineRunner) -> None
        super().__init__()
        self.observer = observer
        self.runner = runner

    def observe(self, update):
        if self.observer.auto_load:
            meta = self.runner.trajectory.experiments[update.index]
            if update.status == Update.DONE:
                meta = meta.fresh_copy()
        else:
            meta = update.meta

        if update.status == Update.STARTED:
            self.observer.experiment_started(update.index, meta)
        if update.status == Update.DONE:
            self.observer.experiment_finished(update.index, meta)
        if update.status == Update.TIMEOUT:
            self.observer.experiment_interrupted(update.index, meta)
        if update.status == Update.FAILED:
            self.observer.experiment_failed(update.index, meta)


class PrintObserver(ProgressObserver):
    def run_started(self, platform, name, run_count, run_date):
        print("[{}] started: {} - {}".format(platform, name, run_count))

    def experiment_started(self, index, experiment):
        print("[{}] started: {}".format(index, experiment))

    def experiment_finished(self, index, experiment):
        print("[{}] done: {}".format(index, experiment))

    def experiment_interrupted(self, index, experiment):
        print("[{}] timed out: {}".format(index, experiment))

    def experiment_failed(self, index, experiment):
        print("[{}] failed: {}".format(index, experiment))

    def run_finished(self, platform, name, run_count, run_date):
        print("[{}] done: {} - {}".format(platform, name, run_count))


class CommandLineRunner(object):
    def __init__(self, trajectory, storage, processes=None, timeout=None, observer=None):
        self.trajectory = trajectory
        self.storage = storage
        self.timeout = timeout
        self.processes = processes
        self.observer = None if observer is None else ParallelToProcess(observer, self)
        self.run_count = self.storage.get_new_run()

    def run(self):
        commands = []
        run_date = datetime.now()
        platform = platform_library.node()
        if self.observer:
            self.observer.observer.run_started(platform, self.trajectory.name, self.run_count, run_date)
        for experiment in self.trajectory.experiments:
            if self.timeout:
                experiment.config["@timeout"] = self.timeout
            experiment.config["@run.count"] = self.run_count
            experiment.config["@run.date"] = run_date
            experiment.config["@run.computer"] = platform
            experiment.save(self.storage)
            storage_name = export_storage(experiment.storage)
            filename = inspect.getfile(experiment.__class__)
            commands.append("python {} {} run {}".format(filename, storage_name, experiment.identifier))
        meta = [e.identifier for e in self.trajectory.experiments] if self.observer else None
        parallel.run_commands(commands, timeout=self.timeout, observer=self.observer, meta=meta)
        if self.observer:
            self.observer.observer.run_finished(platform, self.trajectory.name, self.run_count, run_date)
        return [self.storage.get_experiment(e.__class__, e.identifier) for e in self.trajectory.experiments]


class ParallelRunner(object):
    def __init__(self, trajectory, storage, processes=None, timeout=None):
        self.trajectory = trajectory
        self.storage = storage
        self.timeout = timeout
        self.processes = processes

    def run(self):
        for e in self.trajectory.experiments:
            if self.timeout:
                e.config["@timeout"] = self.timeout
            e.save(self.storage)
        args_list = [(self.storage, e.__class__, e.identifier) for e in self.trajectory.experiments]
        results = []
        with ProcessPool() as pool:
            future = pool.map(ParallelRunner.run_single, args_list, timeout=self.timeout)

            iterator = future.result()

            while True:
                exp_id = None
                try:
                    exp_id = next(iterator)
                    results.append(exp_id)
                    print("[done]    {exp_id}".format(exp_id=exp_id))
                except StopIteration:
                    break
                except (TimeoutError, OtherTimeoutError):
                    print("[timeout] {exp_id} (timeout={timeout})".format(exp_id=exp_id or "-", timeout=self.timeout))

        return [self.storage.get_experiment(oe.__class__, exp_id)
                for exp_id, oe in zip(results, self.trajectory.experiments)]

    @staticmethod
    def run_single(arg_tuple):
        storage, cls, identifier = arg_tuple
        experiment = storage.get_experiment(cls, identifier)
        experiment.run(True)
        return experiment.identifier


def import_runner(runner_string, trajectory, storage, timeout=None):
    if runner_string == "cli":
        return CommandLineRunner(trajectory, storage, timeout=timeout)
    elif runner_string == "multi":
        return ParallelRunner(trajectory, storage, timeout=timeout)
    else:
        raise ValueError("Could not parse runner from {}".format(runner_string))

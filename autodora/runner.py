import inspect
import platform as platform_library
import sys
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Dict

from .observe import ProgressObserver
from .parallel import ParallelObserver, Update
from . import parallel
from .storage import export_storage

if TYPE_CHECKING:
    from .storage import Storage
    from .experiment import Experiment
    from .trajectory import Trajectory


class ParallelToProcess(ParallelObserver):
    def __init__(self, observer, runner):
        # type: (ProgressObserver, CommandLineRunner) -> None
        super().__init__()
        self.observer = observer
        self.runner = runner

    def observe(self, update):
        if self.observer.auto_load:
            meta = self.runner.trajectory.experiments[update.index]
            if update.status == Update.DONE or update.status == Update.FAILED:
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
    def __init__(self, auto_load=True):
        super().__init__(auto_load=auto_load)
        self.experiment_count = None

    def run_started(self, platform, name, run_count, run_date, experiment_count):
        self.experiment_count = experiment_count
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


class PrintCountObserver(ProgressObserver):
    def __init__(self):
        super().__init__(auto_load=False)
        self.completed = self.failed = self.timed_out = 0
        self.name = None
        self.experiment_count = None

    def run_started(self, platform, name, run_count, run_date, experiment_count):
        self.experiment_count = experiment_count
        self.name = name
        self.print_message()

    def experiment_started(self, index, experiment):
        pass

    def experiment_finished(self, index, experiment):
        self.completed += 1
        self.print_message()

    def experiment_interrupted(self, index, experiment):
        self.timed_out += 1
        self.print_message()

    def experiment_failed(self, index, experiment):
        self.failed += 1
        self.print_message()

    def run_finished(self, platform, name, run_count, run_date):
        pass

    def print_message(self):
        print(
            f"[{self.name}] "
            f"{self.completed + self.failed + self.timed_out} / {self.experiment_count} "
            f"(C {self.completed} | E {self.failed} | T {self.timed_out})"
        )


class Runner:
    def __init__(self, trajectory, observer):
        self.trajectory = trajectory  # type: Trajectory
        self.observer = observer

    @staticmethod
    def setting_exists(setting, previous_experiments):
        for e in previous_experiments:
            matches = True
            for k, v in setting.items():
                if e[k] != v:
                    matches = False
            if matches:
                return e
        return None


class StoredRunner(Runner):
    def __init__(self, trajectory, storage, observer, repeat):
        super().__init__(trajectory, observer)
        self.storage = storage  # type: Storage
        self.repeat = repeat
        self._previous_experiments = None
        self.run_count = None if storage is None else self.storage.get_new_run()

    def setting_exists(self, setting, experiment):
        if self.storage is None:
            return False
        if self._previous_experiments is None:
            self._previous_experiments = self.storage.get_experiments(
                experiment.__class__, self.trajectory.name
            )
        return Runner.setting_exists(setting, self._previous_experiments)

    def get_existing(self, setting, experiment):
        # type: (Dict, Experiment) -> Optional[Experiment]
        if not self.repeat:
            return self.setting_exists(setting, experiment)
        return None


class CommandLineRunner(StoredRunner):
    def __init__(
        self,
        trajectory,
        storage,
        processes=None,
        timeout=None,
        observer=None,
        via_cli=True,
        repeat=False,
        cmd=None,
    ):
        super().__init__(
            trajectory,
            storage,
            None if observer is None else ParallelToProcess(observer, self),
            repeat,
        )
        self.timeout = timeout
        self.processes = processes
        self.via_cli = via_cli
        self.cmd = cmd

    def set_observer(self, observer):
        self.observer = ParallelToProcess(observer, self)

    def run(self):
        commands = []
        run_date = datetime.now()
        platform = platform_library.node()
        name = self.trajectory.name

        experiments = []
        for s, e in zip(self.trajectory.settings, self.trajectory.experiments):
            existing = self.get_existing(s, e)
            if existing is None:
                experiments.append(e)
            else:
                e.identifier = existing.identifier

        if self.observer:
            experiment_count = len(experiments)
            self.observer.observer.run_started(
                platform, name, self.run_count, run_date, experiment_count
            )

        for experiment in experiments:
            if self.timeout:
                experiment.config["@timeout"] = self.timeout
            experiment.config["@run.count"] = self.run_count
            experiment.config["@run.date"] = run_date
            experiment.config["@run.computer"] = platform
            experiment.save(self.storage)
            storage_name = export_storage(experiment.storage)
            cls = experiment.__class__
            filename = inspect.getfile(cls)
            if self.via_cli:
                if self.cmd is None:
                    commands.append(
                        "python {} -s {} run {}".format(
                            filename, storage_name, experiment.identifier
                        )
                    )
                else:
                    commands.append(
                        f"{self.cmd} -s {storage_name} run {experiment.identifier}"
                    )
            else:
                commands.append(
                    (
                        CommandLineRunner.run_single,
                        (self.storage, cls, experiment.identifier),
                    )
                )

        meta = [e.identifier for e in experiments] if self.observer else None
        parallel.run_commands(
            commands,
            timeout=self.timeout,
            observer=self.observer,
            meta=meta,
            processes=self.processes,
        )
        if self.observer:
            self.observer.observer.run_finished(
                platform, name, self.run_count, run_date
            )
        return [
            self.storage.get_experiment(e.__class__, e.identifier)
            for e in self.trajectory.experiments
        ]

    @staticmethod
    def run_single(storage, cls, identifier):
        experiment = storage.get_experiment(cls, identifier)
        experiment.run_wrapped(True)
        return experiment.identifier


def import_runner(runner_string, trajectory, storage, timeout=None, cmd=None):
    if runner_string == "cli":
        return CommandLineRunner(trajectory, storage, timeout=timeout, cmd=cmd)
    elif runner_string == "multi":
        return CommandLineRunner(trajectory, storage, timeout=timeout, via_cli=False)
    else:
        raise ValueError("Could not parse runner from {}".format(runner_string))

import inspect

from pebble import ProcessPool

from . import parallel
from .storage import export_storage
from concurrent.futures import TimeoutError as OtherTimeoutError


class CommandLineRunner(object):
    def __init__(self, trajectory, storage, processes=None, timeout=None):
        self.trajectory = trajectory
        self.storage = storage
        self.timeout = timeout
        self.processes = processes

    def run(self):
        commands = []
        for experiment in self.trajectory.experiments:
            if self.timeout:
                experiment.config["timeout"] = self.timeout
            experiment.config["started"] = True
            experiment.save(self.storage)
            storage_name = export_storage(experiment.storage)
            filename = inspect.getfile(experiment.__class__)
            commands.append("python {} {} run {}".format(filename, storage_name, experiment.identifier))
        parallel.run_commands(commands, timeout=self.timeout)
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
                e.config["timeout"] = self.timeout
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

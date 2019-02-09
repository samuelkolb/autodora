from datetime import datetime
import platform as platform_library
from typing import Optional

from .observe import ProgressObserver
from .storage import Storage
from .trajectory import Trajectory


class SimpleRunner(object):
    def __init__(self, trajectory: Trajectory, storage: Storage, observer: Optional[ProgressObserver]=None):
        self.trajectory = trajectory
        self.storage = storage
        self.observer = observer
        self.run_count = None if storage is None else self.storage.get_new_run()

    def run(self):
        run_date = datetime.now()
        platform = platform_library.node()
        name = self.trajectory.name
        if self.observer:
            experiment_count = len(self.trajectory.experiments)
            self.observer.run_started(platform, name, self.run_count, run_date, experiment_count)
        for i, experiment in enumerate(self.trajectory.experiments):
            experiment.config["@run.count"] = self.run_count if self.storage else -1
            experiment.config["@run.date"] = run_date
            experiment.config["@run.computer"] = platform
            if self.storage:
                experiment.save(self.storage)

            if self.observer:
                self.observer.experiment_started(i, experiment)

            # noinspection PyBroadException
            try:
                experiment.run(bool(self.storage))
                if self.observer:
                    self.observer.experiment_finished(i, experiment)
            except Exception:
                if self.observer:
                    self.observer.experiment_failed(i, experiment)

        if self.observer:
            self.observer.run_finished(platform, name, self.run_count, run_date)
        return self.trajectory.experiments

    @staticmethod
    def run_single(storage, cls, identifier):
        experiment = storage.get_experiment(cls, identifier)
        experiment.run(True)
        return experiment.identifier

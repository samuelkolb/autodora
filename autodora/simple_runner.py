from datetime import datetime
import platform as platform_library
from typing import Optional

from .observe import ProgressObserver
from .storage import Storage
from .trajectory import Trajectory
from .runner import StoredRunner


class SimpleRunner(StoredRunner):
    def __init__(self, trajectory: Trajectory, storage: Storage, observer: Optional[ProgressObserver] = None,
                 repeat=False):
        super().__init__(trajectory, storage, observer, repeat)

    def run(self):
        run_date = datetime.now()
        platform = platform_library.node()
        name = self.trajectory.name
        experiments = [e for s, e in zip(self.trajectory.settings, self.trajectory.experiments)
                       if self.get_existing(s, e) is None]
        if self.observer:
            experiment_count = len(self.trajectory.experiments)
            self.observer.run_started(platform, name, self.run_count, run_date, experiment_count)
        for i, experiment in enumerate(experiments):
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

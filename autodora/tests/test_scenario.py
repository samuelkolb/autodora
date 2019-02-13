import os

import pytest

from autodora.observe import ProgressObserver
from autodora.observers.telegram_observer import TelegramObserver
from product_experiment import ProductExperiment
from autodora.trajectory import product
from autodora.runner import CommandLineRunner


@pytest.fixture(scope='session', autouse=True)
def db_conn():
    os.environ["DB"] = os.path.join(os.path.dirname(__file__), "tests.sqlite")

    yield

    os.unlink(os.environ["DB"])


class CountObserver(ProgressObserver):
    def __init__(self, experiment_normal_count, experiment_timeout_count, experiment_error_count):
        super().__init__()
        self.experiment_started_count = experiment_normal_count + experiment_timeout_count + experiment_error_count
        self.experiment_normal_count = experiment_normal_count
        self.experiment_timeout_count = experiment_timeout_count
        self.experiment_error_count = experiment_error_count
        self.started = False
        self.finished = False

    def run_started(self, platform, name, run_count, run_date, experiment_count):
        assert experiment_count == self.experiment_started_count
        assert not self.started
        self.started = True

    def experiment_started(self, index, experiment):
        print("Start", self, self.experiment_started_count)
        assert self.experiment_started_count > 0
        self.experiment_started_count -= 1

    def experiment_finished(self, index, experiment):
        assert self.experiment_normal_count > 0
        self.experiment_normal_count -= 1

    def experiment_interrupted(self, index, experiment):
        assert self.experiment_timeout_count > 0
        self.experiment_timeout_count -= 1

    def experiment_failed(self, index, experiment):
        print(experiment)
        assert self.experiment_error_count > 0
        self.experiment_error_count -= 1

    def run_finished(self, platform, name, run_count, run_date):
        assert not self.finished
        self.finished = True

    def done(self):
        assert self.experiment_started_count == 0
        assert self.experiment_normal_count == 0
        assert self.experiment_timeout_count == 0
        assert self.experiment_error_count == 0
        assert self.started
        assert self.finished


def test_product_scenario():
    from autodora.sql_storage import SqliteStorage

    storage = SqliteStorage()

    assert len(storage.get_groups()) == 0

    name = "name"
    input_options = {"input": ["10x20", "10x30", "10x40"]}
    count_options = {"count": [5, 10, 10000000]}
    timeout = 2

    t = ProductExperiment.explore(name, product(input_options, count_options))

    for e in t.experiments:
        assert e["count"]
        assert e["input"]
        assert e["x"] == int(e["input"].split("x")[0])
        assert e["y"] == int(e["input"].split("x")[1])
        assert e["power"] == ProductExperiment.power.default
        assert e["product"] is None
        assert e["@timeout"] is None
        assert not e["@completed"]
        assert e["@runtime"] is None
        assert e["@runtime_wall"] is None
        assert e["@runtime_process"] is None
        assert e["@start_time"] is None
        assert e["@end_time"] is None
        assert e["@run.count"] is None
        assert e["@run.computer"] is None
        assert e["@run.date"] is None

    assert len(t.experiments) == len(input_options["input"]) * len(count_options["count"])

    observer = CountObserver(len(t.experiments) - len(count_options["count"]), len(count_options["count"]), 0)
    dispatcher = ProgressObserver()
    dispatcher.add_observer(observer)
    try:
        dispatcher.add_observer(TelegramObserver())
    except RuntimeError:
        pass
    finished_experiments = CommandLineRunner(t, storage, timeout=timeout, observer=dispatcher).run()
    assert len(finished_experiments) == len(t.experiments)
    observer.done()
    assert len(storage.get_groups()) == 1

    name2 = "name2"
    CommandLineRunner(ProductExperiment.explore(name2, {}), storage).run()
    last_id = CommandLineRunner(ProductExperiment.explore(name, {}), storage).run()[0].identifier

    assert len(storage.get_groups()) == 2
    assert len(storage.get_experiments(ProductExperiment, name)) == len(t.experiments) + 1
    assert len(storage.get_experiments(ProductExperiment, name2)) == 1

    run_date = None
    for e in storage.get_experiments(ProductExperiment, name):
        print(e)
        should_be_run = (e["count"] < 1000)
        assert e["count"]
        assert e["input"]
        assert e["x"] == int(e["input"].split("x")[0])
        assert e["y"] == int(e["input"].split("x")[1])
        assert e["power"] == ProductExperiment.power.default
        assert (e["product"] == (e["x"] * e["y"]) ** e["power"]) or not should_be_run
        assert e["@timeout"] == (timeout if e.identifier != last_id else None)
        assert e["@completed"] == should_be_run
        assert (e["@runtime"] is None) != should_be_run
        assert (e["@runtime_wall"] is None) != should_be_run
        assert (e["@runtime_process"] is None) != should_be_run
        assert e["@start_time"]
        assert (e["@end_time"] is None) != should_be_run
        assert e["@run.count"] == (1 if e.identifier != last_id else 3)
        assert e["@run.computer"] is not None
        assert e["@run.date"] is not None
        if run_date is None:
            run_date = e["@run.date"]
        elif e.identifier != last_id:
            assert e["@run.date"] == run_date

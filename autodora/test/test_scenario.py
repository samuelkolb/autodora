import os

import pytest

from product_experiment import ProductExperiment
from autodora.trajectory import product
from autodora.runner import CommandLineRunner, PrintObserver


@pytest.fixture(scope='session', autouse=True)
def db_conn():
    os.environ["DB"] = os.path.join(os.path.dirname(__file__), "test.sqlite")

    yield

    os.unlink(os.environ["DB"])


def test_product_scenario():
    from autodora.sql_storage import SqliteStorage

    storage = SqliteStorage()

    assert len(storage.get_groups()) == 0

    name = "name"
    input_options = {"input": ["10x20", "10x30", "10x40"]}
    count_options = {"count": [5, 10, 1000000]}
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

    finished_experiments = CommandLineRunner(t, storage, timeout=timeout).run()
    assert len(finished_experiments) == len(t.experiments)

    assert len(storage.get_groups()) == 1

    name2 = "name2"
    CommandLineRunner(ProductExperiment.explore(name2, {}), storage).run()
    last_id = CommandLineRunner(ProductExperiment.explore(name, {}), storage).run()[0].identifier

    assert len(storage.get_groups()) == 2
    assert len(storage.get_experiments(ProductExperiment, name)) == len(t.experiments) + 1
    assert len(storage.get_experiments(ProductExperiment, name2)) == 1

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

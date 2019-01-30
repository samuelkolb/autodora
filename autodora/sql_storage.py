import os
from functools import partial

from peewee import Model, SqliteDatabase, CharField, BooleanField
from playhouse.fields import PickleField

from .storage import Storage

database = SqliteDatabase(
    os.environ.get("DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "experiments.sqlite"))
)


class BaseModel(Model):
    class Meta:
        database = database


class ExperimentModel(BaseModel):
    cls_name = CharField()
    completed = BooleanField()
    group = CharField()
    config = PickleField()
    parameters = PickleField()
    result = PickleField()
    derived = PickleField()


def class_name(cls):
    return cls.__name__


class SqliteStorage(Storage):
    def __init__(self):
        database.connect()
        database.create_tables([ExperimentModel], safe=True)
        database.close()

    def save(self, experiment):
        if experiment.storage == self and experiment.identifier:
            model = ExperimentModel.get_by_id(experiment.identifier)
            model.completed = experiment.completed
            model.group = experiment.group
            model.config = experiment.config.values
            model.parameters = experiment.parameters.values
            model.result = experiment.result.values
            model.derived = experiment.derived
            model.save()

        elif (not experiment.storage or experiment == self) and not experiment.identifier:
            cls = class_name(experiment.__class__)
            model = ExperimentModel.create(cls_name=cls, group=experiment.group, config=experiment.config.values,
                                           parameters=experiment.parameters.values, result=experiment.result.values,
                                           derived=experiment.derived, completed=experiment.completed)
            experiment.storage = self
            experiment.identifier = model.id
        else:
            if experiment != self:
                raise ValueError("Experiment comes from a different storage")
            else:
                raise ValueError("Experiment is partially instantiated")

    def transform(self, cls, model):
        experiment = cls(model.group, self, identifier=model.id)
        experiment.completed = model.completed
        for key, value in model.config.items():
            experiment.config[key] = value
        for key, value in model.parameters.items():
            experiment.parameters[key] = value
        for key, value in model.result.items():
            experiment.result[key] = value
        for key, value in model.derived.items():
            experiment.derived[key] = value
        return experiment

    def get_experiment(self, cls, identifier):
        model = ExperimentModel.get_by_id(identifier)
        if class_name(cls) == model.cls_name:
            return self.transform(cls, model)
        else:
            raise ValueError("Could not find experiment with id {} and class {} (was {})"
                             .format(identifier, cls, model.cls_name))

    def get_experiments(self, cls, group=None):
        cls_name = class_name(cls)
        if group:
            return list(map(partial(self.transform, cls),
                            ExperimentModel.select()
                            .where(ExperimentModel.cls_name == cls_name, ExperimentModel.group == group)))
        else:
            return list(map(partial(self.transform, cls),
                            ExperimentModel.select().where(ExperimentModel.cls_name == cls_name)))

    def remove(self, group):
        ExperimentModel.delete().where(ExperimentModel.group == group).execute()

    def get_groups(self):
        return [m.group for m in ExperimentModel.select()]

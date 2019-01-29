import time
from argparse import ArgumentParser
from typing import Union, Any, Dict, List

from .trajectory import Trajectory
from .storage import import_storage


class Parameter(object):
    def __init__(self, p_type, default, description=None, name=None, arg_name=None):
        self.name = name
        self.p_type = p_type
        self.default = default
        self.description = description
        self.specific_arg_name = arg_name

    @property
    def arg_name(self):
        return self.specific_arg_name or self.name


class Result(Parameter):
    pass


class Config(Parameter):
    pass


class Group(object):
    def __init__(self, name):
        self.name = name
        self.parameters = dict()  # type: Dict[str, Parameter]
        self.values = dict()  # type: Dict[str, Any]

    def add_parameter(self, name, p_type, default=None, description=None, arg_name=None):
        self.add(Parameter(p_type, default, description, name, arg_name))

    def add(self, parameter):
        self.parameters[parameter.name] = parameter

    def set_values(self, **kwargs):
        for key, value in kwargs.items():
            self.set_value(key, value)

    def set_value(self, name, value):
        if name not in self.parameters:
            raise ValueError(f"No parameter called {name}")
        if not isinstance(value, self.parameters[name].p_type):
            raise ValueError(f"Unexpected type for value {value}")
        self.values[name] = value

    def __setitem__(self, key, value):
        self.set_value(key, value)

    def __getitem__(self, item):
        return self.values[item] if item in self.values else self.parameters[item].default

    def add_arguments(self, parser, prefix=None):
        for name, parameter in self.parameters.items():
            parser.add_argument(
                f"--{prefix or ''}{parameter.arg_name}",
                type=parameter.p_type,
                default=parameter.default,
                help=parameter.description
            )

    def parse_arguments(self, args, prefix=None):
        for name, parameter in self.parameters.items():
            self.set_value(name, getattr(args, f"{prefix or ''}{parameter.arg_name}"))

    def get_arguments(self, prefix=None):
        return " ".join(
            "--{} {}".format(f"{prefix or ''}{self.parameters[name].arg_name}", value)
            for name, value in self.values.items() if value is not None
        )

    def copy(self):
        group = self.__new__(self.__class__)
        group.parameters = dict(self.parameters)
        group.values = dict(self.values)
        return group

    def __str__(self):
        values = ", ".join(f"{key}: {self.values.get(key, '')}" for key in self.parameters)
        return f"{self.name}={{{values}}}"


class Derived(object):
    def __init__(self, callback):
        self.callback = callback

    def __call__(self, *args, **kwargs):
        return self.callback(*args, **kwargs)


def derived(func):
    return Derived(func)


class Experiment(object):
    def __init__(self, group, storage=None, identifier=None):
        self.derived_callbacks = dict()
        self.storage = storage
        self.identifier = identifier
        self.completed = False

        self.group = group
        self.config = Group("config")
        self.config.add_parameter("timeout", int, None, description="The timeout value set for this experiment")
        self.config.add_parameter("started", bool, False, description="Whether this experiment has been started")
        self.parameters = Group("parameters")
        self.result = Group("result")
        self.result.add_parameter("runtime", float, None, description="How long the experiment took to execute")
        self.derived = dict()

        for key, value in self.__class__.__dict__.items():
            if isinstance(value, Parameter):
                if value.name is None:
                    value.name = key
                if isinstance(value, Config):
                    self.config.add(value)
                elif isinstance(value, Result):
                    self.result.add(value)
                else:
                    self.parameters.add(value)
            elif isinstance(value, Derived):
                if key.startswith("derived_"):
                    key = key[8:]
                self.derived_callbacks[key] = value

    def get_derived(self, name):
        if name in self.derived:
            return self.derived[name]

        if name in self.derived_callbacks:
            result = self.derived_callbacks[name](self)
            self.derived[name] = result
            return result

        raise ValueError(f"There is no derived attribute with the name {name}")

    def __getitem__(self, item):
        return self.get(item)

    def __getattr__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.set(key, value)

    def get(self, name: Union[str, Parameter]):
        if isinstance(name, Parameter):
            name = name.name
        parts = name.split(".", 1)
        if parts[0] == "par" or parts[0] == "parameter":
            return self.parameters[parts[1]]
        elif parts[0] == "res" or parts[0] == "result":
            return self.result[parts[1]]
        elif parts[0] == "conf" or parts[0] == "config":
            return self.config[parts[1]]
        elif parts[0] == "derived":
            return self.get_derived(parts[1])
        else:
            name = ".".join(parts)
            results = []
            if name in self.config.parameters:
                results.append(self.config[name])
            elif name in self.parameters.parameters:
                results.append(self.parameters[name])
            elif name in self.result.parameters:
                results.append(self.result[name])
            elif name in self.derived_callbacks:
                results.append(self.get_derived(name))
            if len(results) == 1:
                return results[0]
            elif len(results) > 1:
                raise ValueError(f"Multiple entries found for the name {name}, please use parameter.{name}, "
                                 f"result.{name}, config.{name} or derived.{name} to disambiguate")
        raise ValueError(f"No entry found for the name {name}")

    def set(self, name, value):
        if isinstance(name, Parameter):
            name = name.name

        parts = name.split(".", 1)
        if parts[0] == "par" or parts[0] == "parameter":
            self.parameters[parts[1]] = value
        elif parts[0] == "res" or parts[0] == "result":
            self.result[parts[1]] = value
        elif parts[0] == "conf" or parts[0] == "config":
            self.config[parts[1]] = value
        elif parts[0] == "derived":
            self.get_derived(parts[1])
        else:
            name = ".".join(parts)
            results = []
            if name in self.config.parameters:
                results.append(self.config)
            elif name in self.parameters.parameters:
                results.append(self.parameters)
            elif name in self.result.parameters:
                results.append(self.result)

            if len(results) == 1:
                results[0][name] = value
            elif len(results) > 1:
                raise ValueError(f"Multiple entries found for the name {name}, please use parameter.{name}, "
                                 f"result.{name}, config.{name} or derived.{name} to disambiguate")
            else:
                raise ValueError(f"No entry found for the name {name}")

    def run(self, auto_save=True):
        try:
            start = time.perf_counter()
            self.run_internal()
            runtime = time.perf_counter() - start
            self.result["runtime"] = runtime
            self.completed = True
            if auto_save and self.storage:
                self.save()
            return self
        except KeyboardInterrupt:
            return self

    def run_internal(self):
        raise NotImplementedError()

    def save(self, storage=None):
        if storage:
            storage.save(self)
        else:
            if not self.storage:
                raise ValueError(f"No storage specified")
            self.storage.save(self)

    def __str__(self):
        return f"EXP(group={self.group or ''}, id={self.identifier or ''}, {self.config}, {self.parameters}," \
               f"{self.result}, derived={self.derived})"

    def add_arguments(self, parser, prefix=None):
        self.config.add_arguments(parser, f"{prefix or ''}conf.")
        self.parameters.add_arguments(parser, f"{prefix or ''}par.")
        self.result.add_arguments(parser, f"{prefix or ''}res.")

    def parse_arguments(self, args, prefix=None):
        self.config.parse_arguments(args, f"{prefix or ''}conf.")
        self.parameters.parse_arguments(args, f"{prefix or ''}par.")
        self.result.parse_arguments(args, f"{prefix or ''}res.")

    def get_arguments(self, prefix=None):
        strings = [
            self.config.get_arguments(f"{prefix or ''}conf."),
            self.parameters.get_arguments(f"{prefix or ''}par."),
            self.result.get_arguments(f"{prefix or ''}res."),
        ]
        return " ".join(strings)

    @classmethod
    def explore(cls, name: str, settings: Union[List[Dict[str, Any]], Dict[str, List]]):
        trajectory = Trajectory(name)
        trajectory.explore(cls, settings)
        return trajectory

    @classmethod
    def enable_cli(cls):
        if cls.__module__ == "__main__":
            cls.run_cli()

    @classmethod
    def run_cli(cls):
        from .cli import parse_cli
        parse_cli(cls)


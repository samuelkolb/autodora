from argparse import ArgumentParser
from typing import Type, TYPE_CHECKING

from .analyze import is_excluded_from_string
from .runner import import_runner
from .storage import import_storage
from .analyze import add_arguments, show_from_args

if TYPE_CHECKING:
    from .experiment import Experiment


def parse_cli(cls):
    # type: (Type[Experiment]) -> None
    parser = ArgumentParser()
    parser.add_argument("storage")
    sub_parser = parser.add_subparsers(dest="mode")
    run_parser = sub_parser.add_parser("run")
    run_parser.add_argument("exp_id", type=int)

    analyze_parser = sub_parser.add_parser("analyze")
    analyze_parser.add_argument("-n", "--names", nargs="+", type=str)
    add_arguments(analyze_parser)

    explore_parser = sub_parser.add_parser("explore")
    explore_parser.add_argument("name", type=str, help="The name for this experiment")
    experiment = cls("")
    for parameter in experiment.parameters.parameters.values():
        explore_parser.add_argument("--{}".format(parameter.arg_name), type=parameter.p_type, nargs="+",
                                    help=parameter.description, dest="parameter.{}".format(parameter.arg_name))
    explore_parser.add_argument("-e", type=str, default=None, help="Execute the trajectory with the specified engine")
    explore_parser.add_argument("-t", type=int, default=None, help="Timeout for the execution")

    list_parser = sub_parser.add_parser("list")
    list_parser.add_argument("name", nargs="?", default=None)
    list_parser.add_argument("-e", "--exclude", nargs="+", type=str, default=None)

    remove_parser = sub_parser.add_parser("remove")
    remove_parser.add_argument("name")
    remove_parser.add_argument("-e", "--exclude", nargs="+", type=str, default=None)
    remove_parser.add_argument("--dry_run", action="store_true")

    groups_parser = sub_parser.add_parser("groups")

    # python product_experiment.py sqlite analyze

    args = parser.parse_args()
    storage = import_storage(args.storage)

    if args.mode == "run":
        exp_id = args.exp_id
        experiment = storage.get_experiment(cls, exp_id)
        experiment.run(True)
    elif args.mode == "analyze":
        experiments = []
        for name in args.names:
            for experiment in storage.get_experiments(cls, name):
                experiments.append(experiment)

        try:
            show_from_args(experiments, args)
        except KeyboardInterrupt:
            pass
    elif args.mode == "explore":
        from .trajectory import product, Trajectory
        settings = None
        for parameter in experiment.parameters.parameters.values():
            values = getattr(args, "parameter.{}".format(parameter.arg_name))
            if values:
                local_settings = {parameter.name: values}
                if settings is None:
                    settings = local_settings
                else:
                    settings = product(settings, local_settings)
        trajectory = Trajectory(args.name)
        trajectory.explore(cls, settings)
        print(*trajectory.experiments, sep="\n")
        if args.e:
            engine = import_runner(args.e, trajectory, storage, args.t)
            engine.run()
    elif args.mode == "list":
        if args.name:
            exclusion_filter = args.exclude
            print(*[e for e in storage.get_experiments(cls, args.name)
                    if not exclusion_filter or not any(is_excluded_from_string(f, e) for f in exclusion_filter)],
                  sep="\n")
        else:
            print(*storage.get_groups(), sep="\n")
    elif args.mode == "remove":
        exclusion_filter = args.exclude
        if exclusion_filter:
            for e in storage.get_experiments(cls, args.name):
                for f in exclusion_filter:
                    if not is_excluded_from_string(f, e):
                        storage.remove(args.name, experiment_id=e.identifier, dry_run=args.dry_run)
        else:
            storage.remove(args.name, dry_run=args.dry_run)

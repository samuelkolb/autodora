from argparse import ArgumentParser
from typing import Type, TYPE_CHECKING, Optional

from .observe import ProgressObserver
from .settings import DEFAULT_GROUP_NAME
from .analyze import is_excluded_from_string
from .runner import import_runner, PrintCountObserver
from .storage import import_storage
from .analyze import add_arguments, show_from_args

if TYPE_CHECKING:
    from .experiment import Experiment


def parse_cli(cls, cmd=None):
    # type: (Type[Experiment], Optional[str]) -> None
    parser = ArgumentParser()
    parser.add_argument(
        "-s",
        "--storage",
        default="sqlite",
        type=str,
        help="Which type of storage to use.  Default (and currently only option) is 'sqlite'.",
    )

    sub_parser = parser.add_subparsers(
        dest="mode",
        help="Which action to perform: "
        "[run] Runs a specific experiment (by id), "
        "[analyze] Analyze results, "
        "[explore] Queue experiments to explore parameter values, "
        "[list] Lists experiments in the database, "
        "[remove] Remove experiments from the database",
    )
    run_parser = sub_parser.add_parser("run")
    run_parser.add_argument("exp_id", type=int)

    analyze_parser = sub_parser.add_parser("analyze")
    analyze_parser.add_argument("-n", "--names", nargs="+", type=str)
    add_arguments(analyze_parser)

    explore_parser = sub_parser.add_parser("explore")
    explore_parser.add_argument(
        "-n",
        "--name",
        type=str,
        help="Assign a name to the experiment",
        default=DEFAULT_GROUP_NAME,
    )
    experiment = cls("")
    for parameter in experiment.parameters.parameters.values():
        explore_parser.add_argument(
            "--{}".format(parameter.arg_name),
            type=parameter.p_type if parameter.p_type != bool else int,
            nargs="+",
            help=parameter.description,
            dest="parameter.{}".format(parameter.arg_name),
        )
    explore_parser.add_argument(
        "-e",
        "--engine",
        type=str,
        default=None,
        help="Execute the trajectory with the specified engine",
    )
    explore_parser.add_argument(
        "-t", "--timeout", type=int, default=None, help="Timeout for the execution"
    )

    list_parser = sub_parser.add_parser("list")
    list_parser.add_argument("name", nargs="?", default=None)
    list_parser.add_argument("-e", "--exclude", nargs="+", type=str, default=None)

    remove_parser = sub_parser.add_parser("remove")
    remove_parser.add_argument("name")
    remove_parser.add_argument("-e", "--exclude", nargs="+", type=str, default=None)
    remove_parser.add_argument("--dry_run", action="store_true")

    # groups_parser = sub_parser.add_parser("groups")

    # python product_experiment.py sqlite analyze

    args = parser.parse_args()
    storage = import_storage(args.storage)

    if args.mode == "run":
        exp_id = args.exp_id
        experiment = storage.get_experiment(cls, exp_id)
        experiment.run_wrapped(True)
    elif args.mode == "analyze":
        experiments = []
        names = args.names or [DEFAULT_GROUP_NAME]
        for name in names:
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
            if values and parameter.p_type == bool:
                values = [bool(v) for v in values]
            if values:
                local_settings = {parameter.name: values}
                if settings is None:
                    settings = local_settings
                else:
                    settings = product(settings, local_settings)
        trajectory = Trajectory(args.name)
        trajectory.explore(cls, settings)
        print(*trajectory.experiments, sep="\n")
        if args.engine:
            engine = import_runner(args.engine, trajectory, storage, args.timeout, cmd)
            engine.set_observer(PrintCountObserver())
            engine.run()
    elif args.mode == "list":

        def print_experiments(group_name, exclusion_filter=None):
            experiments_to_print = [
                e
                for e in storage.get_experiments(cls, group_name)
                if not exclusion_filter
                or not any(is_excluded_from_string(f, e) for f in exclusion_filter)
            ]
            if len(experiments_to_print) > 0:
                print(*experiments_to_print, sep="\n")
            else:
                print("No experiments to list.")

        if args.name:
            print_experiments(args.name, args.exclude)
        else:
            groups = storage.get_groups()
            if len(groups) == 0:
                print("No experiments to list.")
            elif len(groups) == 1 and groups[0] == DEFAULT_GROUP_NAME:
                print_experiments(DEFAULT_GROUP_NAME)
            else:
                print(*groups, sep="\n")
    elif args.mode == "remove":
        exclusion_filter = args.exclude
        if exclusion_filter:
            for e in storage.get_experiments(cls, args.name):
                for f in exclusion_filter:
                    if not is_excluded_from_string(f, e):
                        storage.remove(
                            args.name, experiment_id=e.identifier, dry_run=args.dry_run
                        )
        else:
            storage.remove(args.name, dry_run=args.dry_run)

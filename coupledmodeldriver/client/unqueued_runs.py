from argparse import ArgumentParser
from collections import Collection
import os
from pathlib import Path
from typing import Any, Dict, List

from coupledmodeldriver.client.check_completion import (
    check_completion,
    is_model_directory,
    MODELS,
)
from coupledmodeldriver.configure import ModelJSON


def parse_missing_jobs_arguments() -> Dict[str, Any]:
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        'directory',
        nargs='*',
        default=Path.cwd(),
        help='directory containing model run configuration',
    )
    argument_parser.add_argument('--model', help='model that is running, one of: `ADCIRC`')
    argument_parser.add_argument('--dependency', help='job ID of dependency')
    argument_parser.add_argument(
        '--submit', action='store_true', help='whether to submit unqueued runs'
    )

    arguments = argument_parser.parse_args()

    model = arguments.model
    if model is not None:
        model = MODELS[model.lower()]

    return {
        'directories': arguments.directory,
        'model': model,
        'submit': arguments.submit,
        'dependency': arguments.dependency,
    }


def get_run_directories(directories: List[os.PathLike], model: ModelJSON = None) -> List[Path]:
    if not isinstance(directories, Collection):
        directories = [directories]
    run_directories = []
    for directory in directories:
        if not isinstance(directory, Path):
            directory = Path(directory)
        if is_model_directory(directory, model=model):
            run_directories.append(directory)
        else:
            for subdirectory in directory.iterdir():
                if subdirectory.is_dir():
                    run_directories.extend(get_run_directories(subdirectory, model=model))

    return run_directories


def get_unqueued_runs(
    directories: List[os.PathLike], model: ModelJSON = None, **kwargs
) -> Dict[str, Path]:
    """
    get runs in the local configuration that have not been submitted / queued to the job manager

    :param directories: directory containing model run configuration
    :param model: model that is running, one of: ``ADCIRC``
    :return: mapping of unqueued run names to their directory paths
    """

    runs = {
        directory.name: directory
        for directory in get_run_directories(directories, model=model)
    }
    jobs = dict(
        line.split()[:2]
        for line in os.popen('squeue -u $USER -o "%j %Z %i" --sort i').read().splitlines()[1:]
    )

    unqueued_runs = {}
    for run_name, run_directory in runs.items():
        if run_name not in jobs:
            unqueued_runs[run_name] = run_directory

    completion_status = check_completion(unqueued_runs.values(), model=model, verbose=False)

    unqueued_runs = {
        run_name: run_directory
        for run_name, run_directory in unqueued_runs.items()
        if 'not_started' in completion_status[run_directory.name]
    }

    return unqueued_runs


def main():
    """
    submit unqueued runs in the current configuration
    """

    arguments = parse_missing_jobs_arguments()
    directories = arguments['directories']
    model = arguments['model']
    submit = arguments['submit']
    dependency = arguments['dependency']

    unqueued_runs = get_unqueued_runs(directories, model=model)
    starting_directory = Path.cwd()
    if submit:
        unqueued_run_names = []
        for run_name, run_directory in unqueued_runs.items():
            dependencies = "afterok:$(sbatch setup.job | awk '{print $NF}')"
            if dependency is not None:
                dependencies = f'{dependencies}:{dependency}'

            os.chdir(run_directory)
            os.system(f'sbatch --dependency={dependencies} adcirc.job')
            os.chdir(starting_directory)

            unqueued_run_names.append(run_name)
    else:
        unqueued_run_names = list(unqueued_runs)

    for run_name in sorted(unqueued_run_names):
        print(run_name)


if __name__ == '__main__':
    main()

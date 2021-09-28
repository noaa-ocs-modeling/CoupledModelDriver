from argparse import ArgumentParser
from collections import Collection
import os
from pathlib import Path

from coupledmodeldriver.client.check_completion import (
    check_completion,
    is_model_directory,
    MODELS,
)
from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.utilities import LOGGER


def get_run_directories(directories: [os.PathLike], model: ModelJSON = None) -> [Path]:
    if not isinstance(directories, Collection):
        directories = [directories]
    run_directories = []
    for directory in directories:
        if is_model_directory(directory, model=model):
            run_directories.append(directory)
        else:
            for subdirectory in directory.iterdir():
                if subdirectory.is_dir():
                    run_directories.extend(get_run_directories(subdirectory, model=model))

    return run_directories


if __name__ == '__main__':
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        'directory',
        nargs='*',
        default=Path.cwd(),
        help='directory containing model run configuration',
    )
    argument_parser.add_argument('--model', help='model that is running, one of: `ADCIRC`')
    argument_parser.add_argument(
        '--submit', action='store_true', help='whether to submit unsubmitted jobs'
    )

    arguments = argument_parser.parse_args()

    directories = arguments.directory
    model = arguments.model
    submit = arguments.submit

    if model is not None:
        model = MODELS[model.lower()]

    runs = {
        directory.name: directory
        for directory in get_run_directories(directories, model=model)
    }
    jobs = dict(
        line.split()[:2]
        for line in os.popen('squeue -u $USER -o "%j %Z %i" --sort i').read().splitlines()[1:]
    )

    for run_name, run_directory in runs.items():
        if run_name not in jobs:
            if 'not_started' not in check_completion(run_directory, model=model)['status']:
                LOGGER.info(f'missing unstarted run "{run_name}"')
                starting_directory = Path.cwd()
                os.chdir(run_directory)
                command = "sbatch --dependency=afterok:$(sbatch setup.job | awk '{print $NF}') adcirc.job"
                os.system(command)
                os.chdir(starting_directory)

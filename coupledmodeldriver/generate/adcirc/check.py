from glob import glob
import os
from os import PathLike
from pathlib import Path


def tail(file, lines: int = 20) -> [str]:
    """ https://stackoverflow.com/a/136368 """

    total_lines_wanted = lines

    block_size = 1024
    file.seek(0, 2)
    block_end_byte = file.tell()
    lines_to_go = total_lines_wanted
    block_number = -1
    blocks = []
    while lines_to_go > 0 and block_end_byte > 0:
        if block_end_byte - block_size > 0:
            file.seek(block_number * block_size, 2)
            blocks.append(file.read(block_size))
        else:
            file.seek(0, 0)
            blocks.append(file.read(block_end_byte))
        lines_found = blocks[-1].count(b'\n')
        lines_to_go -= lines_found
        block_end_byte -= block_size
        block_number -= 1
    all_read_text = b''.join(reversed(blocks))
    return [line.decode() for line in all_read_text.splitlines()[-total_lines_wanted:]]


def check_adcirc_completion(directory: PathLike = None):
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    required_files = ['fort.14', 'fort.15']
    nonexistant_files = [
        filename for filename in required_files if not (directory / filename).exists()
    ]
    if len(nonexistant_files) > 0:
        raise FileNotFoundError(
            f'not an ADCIRC run directory - file(s) not found: {", ".join(nonexistant_files)}'
        )

    errors = {}

    adcirc_output_log_filename = directory / 'fort.16'
    slurm_error_log_pattern = directory / 'ADCIRC_*_*.err.log'
    slurm_out_log_pattern = directory / 'ADCIRC_*_*.out.log'
    esmf_log_pattern = directory / 'PET*.ESMF_LogFile'
    output_netcdf_pattern = directory / 'fort.*.nc'

    if not adcirc_output_log_filename.exists():
        errors['adcirc_output'] = 'could not find ADCIRC output file `fort.16`'

    for filename in [Path(filename) for filename in glob(str(slurm_error_log_pattern))]:
        with open(filename) as log_file:
            lines = list(log_file.readlines())
            if len(lines) > 0:
                if 'slurm_error' not in errors:
                    errors['slurm_error'] = {}
                if filename.name not in errors['slurm_error']:
                    errors['slurm_error'][filename.name] = []
                errors['slurm_error'][filename.name].extend(lines)

    for filename in [Path(filename) for filename in glob(str(slurm_out_log_pattern))]:
        with open(filename, 'rb') as log_file:
            lines = tail(log_file, lines=3)
            if 'End Epilogue' not in lines[-1]:
                if 'slurm_output' not in errors:
                    errors['slurm_output'] = {}
                if filename.name not in errors['slurm_output']:
                    errors['slurm_output'][filename.name] = []
                errors['slurm_output'][filename.name].extend(lines)

    esmf_log_filenames = [Path(filename) for filename in glob(str(esmf_log_pattern))]
    if len(esmf_log_filenames) > 0:
        for filename in esmf_log_filenames:
            with open(filename) as log_file:
                lines = list(log_file.readlines())
                if len(lines) == 0:
                    if 'esmf_output' not in errors:
                        errors['esmf_output'] = {}
                    errors['esmf_output'][
                        filename.name
                    ] = 'job is still running (no `Epilogue`)'
    else:
        if 'esmf_output' not in errors:
            errors[
                'esmf_output'
            ] = f'no ESMF logfiles found with pattern `{os.path.relpath(esmf_log_pattern, directory)}`'

    for filename in [Path(filename) for filename in glob(str(output_netcdf_pattern))]:
        if filename.name == 'fort.63.nc':
            minimum_file_size = 140884
        elif filename.name == 'fort.64.nc':
            minimum_file_size = 140888
        else:
            minimum_file_size = 140884

        if filename.stat().st_size <= minimum_file_size:
            if 'netcdf_output' not in errors:
                errors['netcdf_output'] = {}
            if filename.name not in errors['netcdf_output']:
                errors['netcdf_output'][
                    filename.name
                ] = f'empty file (size {filename.stat().st_size} is not greater than {minimum_file_size})'

    return errors

from glob import glob
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

    errors = {'directory': directory.name}

    slurm_error_log_filenames = glob(str(directory / 'ADCIRC_*_*.err.log'))
    slurm_out_log_filenames = glob(str(directory / 'ADCIRC_*_*.out.log'))
    esmf_log_filenames = glob(str(directory / 'PET*.ESMF_LogFile'))
    output_netcdf_filenames = glob(str(directory / 'fort.*.nc'))

    for log_filename in slurm_error_log_filenames:
        with open(log_filename) as log_file:
            lines = list(log_file.readlines())
            if len(lines) > 0:
                if 'slurm_error' not in errors:
                    errors['slurm_error'] = {}
                if log_filename.name not in errors['slurm_error']:
                    errors['slurm_error'][log_filename.name] = []
                errors['slurm_error'][log_filename.name].extend(lines)

    for log_filename in slurm_out_log_filenames:
        with open(log_filename, 'rb') as log_file:
            lines = tail(log_file, lines=3)
            if 'End Epilogue' not in lines[-1]:
                if 'slurm_output' not in errors:
                    errors['slurm_output'] = {}
                if log_filename.name not in errors['slurm_output']:
                    errors['slurm_output'][log_filename.name] = []
                errors['slurm_output'][log_filename.name].extend(lines)

    if len(esmf_log_filenames) > 0:
        for log_filename in esmf_log_filenames:
            with open(log_filename) as log_file:
                lines = list(log_file.readlines())
                if len(lines) == 0:
                    if 'esmf_output' not in errors:
                        errors['esmf_output'] = {}
                    if log_filename.name not in errors['esmf_output']:
                        errors['esmf_output'][log_filename.name] = []
                    errors['esmf_output'][log_filename.name].extend(lines)
    else:
        if 'esmf_output' not in errors:
            errors['esmf_output'] = 'no ESMF logfiles found (`PET*.ESMF_LogFile`)'

    for netcdf_filename in output_netcdf_filenames:
        netcdf_filename = Path(netcdf_filename)

        if netcdf_filename.name == 'fort.63.nc':
            minimum_file_size = 140884
        elif netcdf_filename.name == 'fort.64.nc':
            minimum_file_size = 140888
        else:
            minimum_file_size = 140884

        if netcdf_filename.stat().st_size <= minimum_file_size:
            if 'netcdf_output' not in errors:
                errors['netcdf_output'] = {}
            if netcdf_filename.name not in errors['netcdf_output']:
                errors['netcdf_output'][
                    netcdf_filename.name
                ] = f'empty file (size {netcdf_filename.stat().st_size} is not greater than {minimum_file_size})'

    return errors

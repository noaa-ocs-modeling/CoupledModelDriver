from abc import ABC
from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path

from coupledmodeldriver.configure.base import ConfigurationJSON


class ModelJSON(ConfigurationJSON, ABC):
    field_types = {
        'executable': Path,
    }

    def __init__(self, executable: PathLike, **kwargs):
        """
        :param executable: file path to model executable
        """

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(ModelJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['executable'] = executable


class CirculationModelJSON(ModelJSON, ABC):
    field_types = {
        'mesh_files': [Path],
        'modeled_start_time': datetime,
        'modeled_duration': timedelta,
        'modeled_timestep': timedelta,
    }

    def __init__(
        self,
        mesh_files: [PathLike],
        modeled_start_time: datetime,
        modeled_duration: timedelta,
        modeled_timestep: timedelta,
        executable: PathLike,
        **kwargs,
    ):
        """
        :param mesh_files: file path to mesh
        :param modeled_start_time: start time in model run
        :param modeled_duration: duration of model run
        :param modeled_timestep: time interval between model steps
        :param executable: file path to model executable
        """

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(CirculationModelJSON.field_types)

        ModelJSON.__init__(self, executable, **kwargs)

        self['mesh_files'] = mesh_files
        self['modeled_start_time'] = modeled_start_time
        self['modeled_duration'] = modeled_duration
        self['modeled_timestep'] = modeled_timestep

    @property
    def modeled_end_time(self) -> datetime:
        return self['modeled_start_time'] + self['modeled_duration']

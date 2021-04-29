from abc import ABC
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
    }

    def __init__(self, mesh_files: [PathLike], executable: PathLike, **kwargs):
        """
        :param mesh_files: file path to mesh
        :param executable: file path to model executable
        """

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(CirculationModelJSON.field_types)

        ModelJSON.__init__(self, executable, **kwargs)

        self['mesh_files'] = mesh_files

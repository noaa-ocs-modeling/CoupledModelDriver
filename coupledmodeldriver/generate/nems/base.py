from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path

from nemspy import ModelingSystem
from nemspy.model import ModelEntry

from coupledmodeldriver.configure.base import ConfigurationJSON


class NEMSJSON(ConfigurationJSON):
    name = 'nems'
    default_filename = f'configure_nems.json'
    field_types = {
        'executable_path': Path,
        'modeled_start_time': datetime,
        'modeled_end_time': datetime,
        'interval': timedelta,
        'models': [ModelEntry],
        'connections': [[str]],
        'mediations': [str],
        'sequence': [str],
    }

    def __init__(
        self,
        executable_path: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        interval: timedelta = None,
        models: [ModelEntry] = None,
        connections: [[str]] = None,
        mediations: [[str]] = None,
        sequence: [str] = None,
        **kwargs,
    ):
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(NEMSJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['executable_path'] = executable_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['interval'] = interval
        self['models'] = models
        self['connections'] = connections
        self['mediations'] = mediations
        self['sequence'] = sequence

    @property
    def nemspy_modeling_system(self) -> ModelingSystem:
        modeling_system = ModelingSystem(
            start_time=self['modeled_start_time'],
            end_time=self['modeled_end_time'],
            interval=self['interval'],
            **{model.model_type.value.lower(): model for model in self['models']},
        )
        for connection in self['connections']:
            modeling_system.connect(*connection)
        for mediation in self['mediations']:
            modeling_system.mediate(*mediation)

        modeling_system.sequence = self['sequence']

        return modeling_system

    def to_nemspy(self) -> ModelingSystem:
        return self.nemspy_modeling_system

    @classmethod
    def from_nemspy(cls, modeling_system: ModelingSystem, executable_path: PathLike = None):
        if executable_path is None:
            executable_path = 'NEMS.x'
        return cls(
            executable_path=executable_path,
            modeled_start_time=modeling_system.start_time,
            modeled_end_time=modeling_system.end_time,
            interval=modeling_system.interval,
            models=modeling_system.models,
            connections=modeling_system.connections,
            sequence=modeling_system.sequence,
        )

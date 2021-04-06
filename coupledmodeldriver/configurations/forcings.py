from abc import ABC, abstractmethod
from datetime import timedelta
from os import PathLike
from pathlib import Path

from adcircpy import Tides
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.tides import HAMTIDE
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy.model import AtmosphericMeshEntry, ModelEntry, WaveMeshEntry

from .base import ConfigurationJSON


class ForcingJSON(ConfigurationJSON, ABC):
    field_types = {'resource': str}

    def __init__(self, resource: PathLike, fields: {str: type} = None):
        if fields is None:
            fields = {}

        fields.update(self.field_types)
        fields.update(ForcingJSON.field_types)

        ConfigurationJSON.__init__(self, fields=fields)

        try:
            resource = Path(resource).as_posix()
        except:
            pass

        self['resource'] = resource

    @property
    @abstractmethod
    def adcircpy_forcing(self) -> Forcing:
        raise NotImplementedError

    def to_adcircpy(self) -> Forcing:
        return self.adcircpy_forcing

    @classmethod
    @abstractmethod
    def from_adcircpy(cls, forcing: Forcing) -> 'ForcingJSON':
        raise NotImplementedError()


class NEMSCapJSON(ConfigurationJSON, ABC):
    field_types = {
        'processors': int,
        'nems_parameters': {str: str},
    }

    def __init__(self, processors: int, nems_parameters: {str: str} = None):
        super().__init__(fields=self.fields, configuration=self.configuration)
        self.fields.update(NEMSCapJSON.field_types)

        if nems_parameters is None:
            nems_parameters = {}

        self['processors'] = processors
        self['nems_parameters'] = nems_parameters

    @abstractmethod
    def nemspy_entry(self) -> ModelEntry:
        raise NotImplementedError()


class TidalForcingJSON(ForcingJSON):
    name = 'tidal_forcing'
    default_filename = f'configure_tidal_forcing.json'
    field_types = {'tidal_source': TidalSource, 'constituents': [str]}

    def __init__(
        self,
        resource: PathLike = None,
        tidal_source: TidalSource = TidalSource.TPXO,
        constituents: [str] = None,
    ):
        if constituents is None:
            constituents = 'ALL'
        elif not isinstance(constituents, str):
            constituents = list(constituents)

        super().__init__(resource=resource)

        self['tidal_source'] = tidal_source
        self['constituents'] = constituents

    @property
    def adcircpy_forcing(self) -> Forcing:
        tides = Tides(tidal_source=self['tidal_source'], resource=self['resource'])

        constituents = [constituent.capitalize() for constituent in self['constituents']]

        if sorted(constituents) == sorted(
            constituent.capitalize() for constituent in tides.constituents
        ):
            constituents = ['All']
        elif sorted(constituents) == sorted(
            constituent.capitalize() for constituent in tides.major_constituents
        ):
            constituents = ['Major']

        if 'All' in constituents:
            tides.use_all()
        else:
            if 'Major' in constituents:
                tides.use_major()
                constituents.remove('Major')
            for constituent in constituents:
                if constituent not in tides.active_constituents:
                    tides.use_constituent(constituent)

        self['constituents'] = list(tides.active_constituents)
        return tides

    @classmethod
    def from_adcircpy(cls, forcing: Tides) -> 'TidalForcingJSON':
        # TODO: workaround for this issue: https://github.com/JaimeCalzadaNOAA/adcircpy/pull/70#discussion_r607245713
        resource = forcing.tidal_dataset.path
        if resource == HAMTIDE.OPENDAP_URL:
            resource = None

        return cls(
            resource=resource,
            tidal_source=forcing.tidal_source,
            constituents=forcing.active_constituents,
        )


class WindForcingJSON(ForcingJSON, ABC):
    field_types = {
        'nws': int,
        'modeled_timestep': timedelta,
    }

    def __init__(
        self,
        resource: PathLike,
        nws: int,
        modeled_timestep: timedelta,
        fields: {str: type} = None
    ):
        super().__init__(resource, fields)
        self.fields.update(WindForcingJSON.field_types)

        self['nws'] = nws
        self['modeled_timestep'] = modeled_timestep


class ATMESHForcingJSON(WindForcingJSON, NEMSCapJSON):
    name = 'atmesh'
    default_filename = f'configure_atmesh.json'

    def __init__(
        self,
        resource: PathLike,
        nws: int = 17,
        modeled_timestep: timedelta = timedelta(hours=1),
        processors: int = 1,
        nems_parameters: {str: str} = None,
    ):
        WindForcingJSON.__init__(self, resource=resource, nws=nws, modeled_timestep=modeled_timestep)
        NEMSCapJSON.__init__(self, processors=processors, nems_parameters=nems_parameters)

    @property
    def adcircpy_forcing(self) -> Forcing:
        return AtmosphericMeshForcing(
            filename=self['resource'],
            nws=self['nws'],
            interval_seconds=self['modeled_timestep'] / timedelta(seconds=1),
        )

    @classmethod
    def from_adcircpy(cls, forcing: AtmosphericMeshForcing) -> 'ATMESHForcingJSON':
        return cls(
            resource=forcing.filename, nws=forcing.NWS, modeled_timestep=forcing.interval,
        )

    @property
    def nemspy_entry(self) -> AtmosphericMeshEntry:
        return AtmosphericMeshEntry(
            filename=self['resource'], processors=self['processors'], **self['nems_parameters']
        )


class WaveForcingJSON(ForcingJSON, ABC):
    field_types = {'nrs': int, 'modeled_timestep': timedelta}

    def __init__(
        self,
        resource: PathLike,
        nrs: int,
        modeled_timestep: timedelta,
        fields: {str: type} = None
    ):
        super().__init__(resource, fields)
        self.fields.update(WaveForcingJSON.field_types)

        self['nrs'] = nrs
        self['modeled_timestep'] = modeled_timestep


class WW3DATAForcingJSON(WaveForcingJSON, NEMSCapJSON):
    name = 'ww3data'
    default_filename = f'configure_ww3data.json'

    def __init__(
        self,
        resource: PathLike,
        nrs: int = 5,
        modeled_timestep: timedelta = timedelta(hours=1),
        processors: int = 1,
        nems_parameters: {str: str} = None,
    ):
        WaveForcingJSON.__init__(self, resource=resource, nrs=nrs, modeled_timestep=modeled_timestep)
        NEMSCapJSON.__init__(self, processors=processors, nems_parameters=nems_parameters)

    @property
    def adcircpy_forcing(self) -> Forcing:
        return WaveWatch3DataForcing(
            filename=self['resource'],
            nrs=self['nrs'],
            interval_seconds=self['modeled_timestep'],
        )

    @classmethod
    def from_adcircpy(cls, forcing: WaveWatch3DataForcing) -> 'WW3DATAForcingJSON':
        return cls(
            resource=forcing.filename, nrs=forcing.NRS, modeled_timestep=forcing.interval,
        )

    @property
    def nemspy_entry(self) -> WaveMeshEntry:
        return WaveMeshEntry(
            filename=self['resource'], processors=self['processors'], **self['nems_parameters']
        )

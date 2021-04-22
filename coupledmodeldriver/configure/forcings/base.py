from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
import sys
from typing import Any

from adcircpy import Tides
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.tides import HAMTIDE
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds import BestTrackForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from adcircpy.forcing.winds.owi import OwiForcing
from nemspy.model import AtmosphericMeshEntry, WaveMeshEntry

from coupledmodeldriver.configure.base import AttributeJSON, \
    ConfigurationJSON, NEMSCapJSON
from coupledmodeldriver.utilities import LOGGER

ADCIRCPY_FORCINGS = {
    'Tides': 'TidalForcingJSON',
    'AtmosphericMeshForcing': 'ATMESHForcingJSON',
    'BestTrackForcing': 'BestTrackForcingJSON',
    'OWIForcing': 'OWIForcingJSON',
    'WaveWatch3DataForcing': 'WW3DATAForcingJSON',
}

ADCIRCPY_FORCING_CLASSES = (Forcing, Tides)


class ForcingJSON(ConfigurationJSON, ABC):
    @property
    @abstractmethod
    def adcircpy_forcing(self) -> Forcing:
        raise NotImplementedError

    def to_adcircpy(self) -> Forcing:
        return self.adcircpy_forcing

    @classmethod
    @abstractmethod
    def from_adcircpy(cls, forcing: Forcing) -> 'ForcingJSON':
        forcing_class_name = forcing.__class__.__name__
        if forcing_class_name in ADCIRCPY_FORCINGS:
            configuration_class = getattr(
                sys.modules[__name__], ADCIRCPY_FORCINGS[forcing_class_name]
            )
            return configuration_class.from_adcircpy(forcing)
        else:
            raise NotImplementedError()


class TimestepForcingJSON(ForcingJSON, ABC):
    default_modeled_timestep: timedelta
    field_types = {'modeled_timestep': timedelta}

    def __init__(self, modeled_timestep: timedelta = None, **kwargs):
        if modeled_timestep is None:
            modeled_timestep = self.default_modeled_timestep
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(TimestepForcingJSON.field_types)

        ForcingJSON.__init__(self, **kwargs)

        self['modeled_timestep'] = modeled_timestep


class FileForcingJSON(ForcingJSON, ABC):
    field_types = {'resource': Path}

    def __init__(self, resource: PathLike, **kwargs):
        if resource is None:
            LOGGER.warning(
                f'resource path not specified for "{self.default_filename}"; '
                f'update entry before generating configuration'
            )
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(FileForcingJSON.field_types)

        ForcingJSON.__init__(self, **kwargs)

        self['resource'] = resource


class TidalForcingJSON(FileForcingJSON):
    name = 'TidalForcing'
    default_filename = f'configure_tidalforcing.json'
    field_types = {'tidal_source': TidalSource, 'constituents': [str]}

    def __init__(
        self,
        resource: PathLike = None,
        tidal_source: TidalSource = TidalSource.TPXO,
        constituents: [str] = None,
        **kwargs,
    ):
        if constituents is None:
            constituents = 'All'
        elif not isinstance(constituents, str):
            constituents = list(constituents)
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(TidalForcingJSON.field_types)

        FileForcingJSON.__init__(self, resource=resource, **kwargs)

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
    default_nws: int
    field_types = {'nws': int}

    def __init__(self, nws: int = None, **kwargs):
        if nws is None:
            nws = self.default_nws
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(WindForcingJSON.field_types)

        ForcingJSON.__init__(self, **kwargs)

        self['nws'] = nws


BESTTRACK_ATTRIBUTES = [
    'NWS',
    'BLADj',
    '_storm_id',
    'geofactor',
    'start_date',
    'end_date',
]


class BestTrackForcingJSON(WindForcingJSON, AttributeJSON):
    name = 'BestTrack'
    default_filename = f'configure_besttrack.json'
    default_nws = 20
    default_attributes = BESTTRACK_ATTRIBUTES
    field_types = {
        'storm_id': str,
        'start_date': datetime,
        'end_date': datetime,
        'fort22_filename': Path,
    }

    def __init__(
        self,
        storm_id: str = None,
        nws: int = None,
        start_date: datetime = None,
        end_date: datetime = None,
        fort22_filename: PathLike = None,
        attributes: {str: Any} = None,
        **kwargs,
    ):
        if storm_id is None and fort22_filename is None:
            raise TypeError("function missing required argument 'storm_id'")

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(BestTrackForcingJSON.field_types)

        WindForcingJSON.__init__(self, nws=nws, **kwargs)
        AttributeJSON.__init__(self, attributes=attributes, **kwargs)

        self['storm_id'] = storm_id
        self['start_date'] = start_date
        self['end_date'] = end_date
        self['fort22_filename'] = fort22_filename

    @property
    def adcircpy_forcing(self) -> BestTrackForcing:
        if self['fort22_filename'] is not None:
            forcing = BestTrackForcing.from_fort22(self['fort22_filename'], self['nws'])
            if self['storm_id'] is not None:
                forcing._storm_id = self['storm_id']
            if ['start_date'] is not None:
                forcing.start_date = self['start_date']
            if ['end_date'] is not None:
                forcing.end_date = self['end_date']
        else:
            forcing = BestTrackForcing(
                storm_id=self['storm_id'],
                nws=self['nws'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )

        for name, value in self['attributes'].items():
            if value is not None:
                try:
                    setattr(forcing, name, value)
                except:
                    LOGGER.warning(
                        f'could not set `{forcing.__class__.__name__}` attribute `{name}` to `{value}`'
                    )

        return forcing

    @classmethod
    def from_adcircpy(cls, forcing: BestTrackForcing) -> 'BestTrackForcingJSON':
        return cls(
            storm_id=forcing.storm_id,
            nws=forcing.NWS,
            start_date=forcing.start_date,
            end_date=forcing.end_date,
        )

    @classmethod
    def from_fort22(cls, filename: PathLike, nws: int = None):
        return cls.from_adcircpy(BestTrackForcing.from_fort22(filename, nws))


class OWIForcingJSON(WindForcingJSON, TimestepForcingJSON):
    name = 'OWI'
    default_filename = f'configure_owi.json'
    default_nws = 12
    default_modeled_timestep = timedelta(hours=1)

    def __init__(self, modeled_timestep: timedelta = None, **kwargs):
        WindForcingJSON.__init__(self, nws=None, **kwargs)
        TimestepForcingJSON.__init__(self, modeled_timestep=modeled_timestep, **kwargs)

    @property
    def adcircpy_forcing(self) -> OwiForcing:
        return OwiForcing(interval_seconds=self['modeled_timestep'] / timedelta(seconds=1))

    @classmethod
    def from_adcircpy(cls, forcing: OwiForcing) -> 'OWIForcingJSON':
        return cls(modeled_timestep=timedelta(seconds=forcing.interval))


class ATMESHForcingJSON(WindForcingJSON, FileForcingJSON, TimestepForcingJSON, NEMSCapJSON):
    name = 'ATMESH'
    default_filename = f'configure_atmesh.json'
    default_nws = 17
    default_modeled_timestep: timedelta
    default_processors = 1

    def __init__(
        self,
        resource: PathLike,
        nws: int = None,
        modeled_timestep: timedelta = None,
        processors: int = None,
        nems_parameters: {str: str} = None,
        **kwargs,
    ):
        WindForcingJSON.__init__(self, nws=nws, **kwargs)
        FileForcingJSON.__init__(self, resource=resource, **kwargs)
        NEMSCapJSON.__init__(
            self, processors=processors, nems_parameters=nems_parameters, **kwargs
        )
        TimestepForcingJSON.__init__(self, modeled_timestep=modeled_timestep, **kwargs)

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
    default_nrs: int
    field_types = {'nrs': int}

    def __init__(self, nrs: int = None, **kwargs):
        if nrs is None:
            nrs = self.default_nrs
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(WaveForcingJSON.field_types)

        ForcingJSON.__init__(self, **kwargs)

        self['nrs'] = nrs


class WW3DATAForcingJSON(WaveForcingJSON, FileForcingJSON, TimestepForcingJSON, NEMSCapJSON):
    name = 'WW3DATA'
    default_filename = f'configure_ww3data.json'
    default_nrs = 5
    default_modeled_timestep = timedelta(hours=1)
    default_processors = 1

    def __init__(
        self,
        resource: PathLike,
        nrs: int = None,
        modeled_timestep: timedelta = None,
        processors: int = None,
        nems_parameters: {str: str} = None,
        **kwargs,
    ):
        WaveForcingJSON.__init__(self, nrs=nrs, **kwargs)
        FileForcingJSON.__init__(self, resource=resource, **kwargs)
        NEMSCapJSON.__init__(
            self, processors=processors, nems_parameters=nems_parameters, **kwargs
        )
        TimestepForcingJSON.__init__(self, modeled_timestep=modeled_timestep, **kwargs)

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

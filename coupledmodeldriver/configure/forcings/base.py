from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import IntEnum, Enum
from os import PathLike
from pathlib import Path
import sys
from typing import Any, Dict, List

from adcircpy import Tides as ADCIRCPyTides
from adcircpy.forcing.base import Forcing as ADCIRCPyForcing
from adcircpy.forcing.tides import HAMTIDE as ADCIRCPyHAMTIDE
from adcircpy.forcing.tides.tides import TidalSource as ADCIRCPyTidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing as ADCIRCPyWaveWatch3DataForcing
from adcircpy.forcing.winds import BestTrackForcing as ADCIRCPyBestTrackForcing
from adcircpy.forcing.winds.atmesh import (
    AtmosphericMeshForcing as ADCIRCPyAtmosphericMeshForcing,
)
from adcircpy.forcing.winds.owi import OwiForcing as ADCIRCPyOwiForcing
from nemspy.model import AtmosphericForcingEntry, WaveWatch3ForcingEntry
from pandas import DataFrame
from pyschism.forcing.bctides.tides import Tides as PySCHISMTides
from pyschism.forcing.bctides.tpxo import TPXO_ELEVATION as PySCHISMTPXO_ELEV
from pyschism.forcing.bctides.tpxo import TPXO_VELOCITY as PySCHISMTPXO_VEL
from pyschism.forcing.bctides.tides import TidalDatabase as PySCHISMTidalDatabase
from pyschism.forcing.nws import BestTrackForcing as PySCHISMBestTrackForcing
from pyschism.forcing.base import ModelForcing as PySCHISMForcing
from pyschism.forcing import NWM as PySCHISMNWM
from pyschism.forcing.source_sink.nwm import NWMElementPairings

from coupledmodeldriver.configure.base import (
    AttributeJSON,
    ConfigurationJSON,
    NEMSCapJSON,
    NoRelPath,
)
from coupledmodeldriver.configure.models import Model
from coupledmodeldriver.utilities import LOGGER

ADCIRCPY_FORCINGS = {
    'Tides': 'TidalForcingJSON',
    'AtmosphericMeshForcing': 'ATMESHForcingJSON',
    'BestTrackForcing': 'BestTrackForcingJSON',
    'OWIForcing': 'OWIForcingJSON',
    'WaveWatch3DataForcing': 'WW3DATAForcingJSON',
}

ADCIRCPY_FORCING_CLASSES = (ADCIRCPyForcing, ADCIRCPyTides)

PYSCHISM_FORCINGS = {
    'Tides': 'TidalForcingJSON',
    'BestTrackForcing': 'BestTrackForcingJSON',
    'NationalWaterModel': 'NationalWaterModelFocringJSON'
    # 'NWM : ,
    # 'GFS, etc.': 'ATMESHForcingJSON',
    # 'AtmosphericMeshForcing': 'ATMESHForcingJSON',
}

PYSCHISM_FORCING_CLASSES = (PySCHISMTides, PySCHISMForcing, PySCHISMNWM)


class TidalSource(IntEnum):
    TPXO = 1
    HAMTIDE = 2


MODEL_TIDAL_SOURCE = {
    TidalSource.TPXO: {
        Model.ADCIRC: ADCIRCPyTidalSource.TPXO,
        Model.SCHISM: PySCHISMTidalDatabase.TPXO,
    },
    TidalSource.HAMTIDE: {
        Model.ADCIRC: ADCIRCPyTidalSource.HAMTIDE,
        Model.SCHISM: PySCHISMTidalDatabase.HAMTIDE,
    },
}


class ForcingJSON(ConfigurationJSON, ABC):
    """
    abstraction of a forcing configuration
    """

    @property
    @abstractmethod
    def adcircpy_forcing(self) -> ADCIRCPyForcing:
        """
        create an ADCIRCpy forcing object with values from this configuration
        """

        raise NotImplementedError

    def to_adcircpy(self) -> ADCIRCPyForcing:
        return self.adcircpy_forcing

    @classmethod
    @abstractmethod
    def from_adcircpy(cls, forcing: ADCIRCPyForcing) -> 'ForcingJSON':
        """
        read configuration values from an ADCIRCpy forcing object
        """

        forcing_class_name = forcing.__class__.__name__
        if forcing_class_name in ADCIRCPY_FORCINGS:
            configuration_class = getattr(
                sys.modules[__name__], ADCIRCPY_FORCINGS[forcing_class_name]
            )
            return configuration_class.from_adcircpy(forcing)
        else:
            raise NotImplementedError()

    @property
    @abstractmethod
    def pyschism_forcing(self) -> PySCHISMForcing:
        """
        create an pySCHISM forcing object with values from this configuration
        """

        raise NotImplementedError

    def to_pyschism(self) -> PySCHISMForcing:
        return self.pyschism_forcing

    @classmethod
    @abstractmethod
    def from_pyschism(cls, forcing: PySCHISMForcing) -> 'ForcingJSON':
        """
        read configuration values from an pySCHISM forcing object
        """

        forcing_class_name = forcing.__class__.__name__
        if forcing_class_name in PYSCHISM_FORCINGS:
            configuration_class = getattr(
                sys.modules[__name__], PYSCHISM_FORCINGS[forcing_class_name]
            )
            return configuration_class.from_pyschism(forcing)
        else:
            raise NotImplementedError()


class TimestepForcingJSON(ForcingJSON, ABC):
    """
    abstraction of a forcing configuration with an arbitrary timestep interval
    """

    default_interval: timedelta
    field_types = {'interval': timedelta}

    def __init__(self, interval: timedelta = None, **kwargs):
        if interval is None:
            interval = self.default_interval
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(TimestepForcingJSON.field_types)

        ForcingJSON.__init__(self, **kwargs)

        self['interval'] = interval


class FileForcingJSON(ForcingJSON, ABC):
    """
    abstraction of a forcing configuration tied to a forcing file on disk
    """

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


class FileGenForcingJSON(ForcingJSON, ABC):
    """
    abstraction of a forcing configuration tied to a file on disk which is
    used to generate the forcing
    """

    field_types = {'resource': NoRelPath}

    def __init__(self, resource: PathLike, **kwargs):
        if resource is None:
            LOGGER.warning(
                f'resource path not specified for "{self.default_filename}"; '
                f'update entry before generating configuration'
            )
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(FileGenForcingJSON.field_types)

        ForcingJSON.__init__(self, **kwargs)

        self['resource'] = resource


class TidalForcingJSON(FileGenForcingJSON):
    """
    tidal configuration in ``configure_tidal.json``

    stores tidal database and constituent information

    .. code-block:: python

        configuration = TidalForcingJSON(
            tidal_source='HAMTIDE',
            constituents='all',
        )

    """

    name = 'TIDAL'
    default_filename = f'configure_tidal.json'
    field_types = {'tidal_source': TidalSource, 'constituents': List[str]}

    def __init__(
        self,
        resource: PathLike = None,
        tidal_source: TidalSource = None,
        constituents: List[str] = None,
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
    def adcircpy_forcing(self) -> ADCIRCPyForcing:
        tides = ADCIRCPyTides(
            tidal_source=MODEL_TIDAL_SOURCE[self['tidal_source']][Model.ADCIRC],
            resource=self['resource'],
        )

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
    def from_adcircpy(cls, forcing: ADCIRCPyTides) -> 'TidalForcingJSON':
        # TODO: workaround for this issue: https://github.com/JaimeCalzadaNOAA/adcircpy/pull/70#discussion_r607245713
        resource = forcing.tidal_dataset.path
        if resource == ADCIRCPyHAMTIDE.OPENDAP_URL:
            resource = None

        tidal_source = getattr(TidalSource, forcing.tidal_source.name)

        return cls(
            resource=resource,
            tidal_source=tidal_source,
            constituents=forcing.active_constituents,
        )

    @property
    def pyschism_forcing(self) -> PySCHISMTides:

        # Setup resource
        if self['tidal_source'] == TidalSource.TPXO:
            # TODO: What if h and uv files are in different locations?
            tidal_database_kwargs = dict(
                h_file=self['resource'].parent / PySCHISMTPXO_ELEV,
                u_file=self['resource'].parent / PySCHISMTPXO_VEL,
            )

        elif self['tidal_source'] == TidalSource.HAMTIDE:
            tidal_database_kwargs = {'resource': self['resource']}
        else:
            raise ValueError(f"PySCHISM doesn't support {self['tidal_source']}!")

        # Setup tidal database
        tidal_database = MODEL_TIDAL_SOURCE[self['tidal_source']][Model.SCHISM].value(
            **tidal_database_kwargs
        )

        # Setup tidal constituents
        constituents = self['constituents']

        if sorted(constituents) == sorted(
            constituent.capitalize() for constituent in PySCHISMTides.constituents
        ):
            constituents = 'all'
        elif sorted(constituents) == sorted(
            constituent.capitalize() for constituent in PySCHISMTides.major_constituents
        ):
            constituents = 'major'
        if constituents[0].lower() in ['all', 'major', 'minor']:
            constituents = list(map(str.lower, constituents))

        tides = PySCHISMTides(tidal_database=tidal_database, constituents=constituents)

        self['constituents'] = list(tides.active_constituents)
        return tides

    @classmethod
    def from_pyschism(cls, forcing: PySCHISMTides) -> 'TidalForcingJSON':

        tidal_source = getattr(TidalSource, forcing.tidal_database.__class__.__name__)

        if tidal_source == TidalSource.TPXO:
            resource = forcing.tidal_database.h.filepath()
        elif tidal_source == TidalSource.HAMTIDE:
            # Local resource for hamtide is not yet implemented in pyschism
            resource = None
        else:
            raise ValueError(f'Invalid tidal source: {tidal_source}')

        return cls(
            resource=resource,
            tidal_source=tidal_source,
            constituents=forcing.active_constituents,
        )


class WindForcingJSON(ForcingJSON, ABC):
    """
    abstraction of a wind forcing configuration

    stores NWS parameter
    """

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
    'BLADj',
    'geofactor',
    'start_date',
    'end_date',
]


class BestTrackForcingJSON(WindForcingJSON, AttributeJSON):
    """
    storm best track configuration in ``configure_besttrack.json``

    stores storm NHC code, NWS parameter, forcing read interval, start and end dates, and optionally a path to an existing `fort.22` file

    .. code-block:: python

        configuration = BestTrackForcingJSON(nhc_code='florence2018')

        configuration = BestTrackForcingJSON.from_fort22('./fort.22')

        configuration.to_adcircpy().write('output.fort.22')

    """

    name = 'BestTrack'
    default_filename = f'configure_besttrack.json'
    default_nws = 20
    default_attributes = BESTTRACK_ATTRIBUTES
    field_types = {
        'nhc_code': str,
        'interval': timedelta,
        'start_date': datetime,
        'end_date': datetime,
        'fort22_filename': NoRelPath,
    }

    def __init__(
        self,
        nhc_code: str = None,
        nws: int = None,
        interval: timedelta = None,
        start_date: datetime = None,
        end_date: datetime = None,
        fort22_filename: PathLike = None,
        dataframe: DataFrame = None,
        attributes: Dict[str, Any] = None,
        **kwargs,
    ):
        if nhc_code is None and fort22_filename is None and dataframe is None:
            LOGGER.warning("no 'nhc_code' given")

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(BestTrackForcingJSON.field_types)

        WindForcingJSON.__init__(self, nws=nws, **kwargs)
        AttributeJSON.__init__(self, attributes=attributes, **kwargs)

        self['nhc_code'] = nhc_code
        self['interval'] = interval
        self['start_date'] = start_date
        self['end_date'] = end_date
        self['fort22_filename'] = fort22_filename

        self.__dataframe = dataframe

        if self.__dataframe is not None:
            forcing = self.adcircpy_forcing
            self['nhc_code'] = forcing.nhc_code
            self['interval'] = forcing.interval
            self['start_date'] = forcing.start_date
            self['end_date'] = forcing.end_date

    @property
    def adcircpy_forcing(self) -> ADCIRCPyBestTrackForcing:
        if self['fort22_filename'] is not None:
            forcing = ADCIRCPyBestTrackForcing.from_fort22(
                self['fort22_filename'],
                nws=self['nws'],
                interval_seconds=self['interval'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
            if self['nhc_code'] is not None and forcing.nhc_code != self['nhc_code']:
                try:
                    forcing.nhc_code = self['nhc_code']
                    self['nhc_code'] = forcing.nhc_code
                except ConnectionError:
                    pass
        elif self.__dataframe is not None:
            forcing = ADCIRCPyBestTrackForcing(
                storm=self.__dataframe,
                nws=self['nws'],
                interval_seconds=self['interval'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
        elif self['nhc_code'] is not None:
            forcing = ADCIRCPyBestTrackForcing(
                storm=self['nhc_code'],
                nws=self['nws'],
                interval_seconds=self['interval'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
        else:
            raise ValueError(
                f'could not create `{ADCIRCPyBestTrackForcing.__name__}` object from given information'
            )

        if self['nhc_code'] is None:
            self[
                'nhc_code'
            ] = f'{forcing.basin}{forcing.storm_number}{forcing.start_date.year}'

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
    def from_adcircpy(cls, forcing: ADCIRCPyBestTrackForcing) -> 'BestTrackForcingJSON':
        return cls(
            nhc_code=forcing.nhc_code,
            nws=forcing.NWS,
            interval=forcing.interval,
            start_date=forcing.start_date,
            end_date=forcing.end_date,
            fort22_filename=forcing.filename,
            dataframe=forcing.data,
        )

    @property
    def pyschism_forcing(self) -> PySCHISMForcing:
        if self['fort22_filename'] is not None:
            forcing = PySCHISMBestTrackForcing.from_nhc_bdeck(
                self['fort22_filename'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
            if self['nhc_code'] is not None and forcing.nhc_code != self['nhc_code']:
                try:
                    forcing.nhc_code = self['nhc_code']
                    self['nhc_code'] = forcing.nhc_code
                except ConnectionError:
                    pass
        elif self.__dataframe is not None:
            forcing = PySCHISMBestTrackForcing(
                storm=self.__dataframe,
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
        elif self['nhc_code'] is not None:
            forcing = PySCHISMBestTrackForcing(
                storm=self['nhc_code'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
        else:
            raise ValueError(
                f'could not create `{PySCHISMBestTrackForcing.__name__}` object from given information'
            )

        if self['nhc_code'] is None:
            self[
                'nhc_code'
            ] = f'{forcing.basin}{forcing.storm_number}{forcing.start_date.year}'

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
    def from_pyschism(cls, forcing: PySCHISMForcing) -> 'ForcingJSON':
        return cls(
            nhc_code=forcing.nhc_code,
            start_date=forcing.start_date,
            end_date=forcing.end_date,
            dataframe=forcing.data,
            fort22_filename=forcing.filename,
        )

    @classmethod
    def from_fort22(
        cls,
        filename: PathLike,
        nws: int = None,
        interval_seconds: int = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ):
        # NOTE: The forcing object is just a temporary, so it doesn't
        # matter if it's from adcircpy or pyschism
        forcing = ADCIRCPyBestTrackForcing.from_fort22(
            filename,
            nws=nws,
            interval_seconds=interval_seconds,
            start_date=start_date,
            end_date=end_date,
        )

        return cls(
            nhc_code=forcing.nhc_code,
            nws=forcing.NWS,
            interval=forcing.interval,
            start_date=forcing.start_date,
            end_date=forcing.end_date,
            fort22_filename=filename,
        )

    from_nhc_bdeck = from_fort22

    def __copy__(self) -> 'BestTrackForcingJSON':
        instance = super().__copy__()
        instance.__class__ = self.__class__
        instance.__dataframe = self.__dataframe
        return instance


class OWIForcingJSON(WindForcingJSON, TimestepForcingJSON):
    """
    OWI forcing configuration in ``configure_owi.json``

    stores NWS parameter and forcing read interval
    """

    name = 'OWI'
    default_filename = f'configure_owi.json'
    default_nws = 12
    default_interval = timedelta(hours=1)

    def __init__(self, interval: timedelta = None, **kwargs):
        WindForcingJSON.__init__(self, nws=None, **kwargs)
        TimestepForcingJSON.__init__(self, interval=interval, **kwargs)

    @property
    def adcircpy_forcing(self) -> ADCIRCPyOwiForcing:
        return ADCIRCPyOwiForcing(interval_seconds=self['interval'] / timedelta(seconds=1))

    @classmethod
    def from_adcircpy(cls, forcing: ADCIRCPyOwiForcing) -> 'OWIForcingJSON':
        return cls(interval=timedelta(seconds=forcing.interval))

    @property
    def pyschism_forcing(self) -> PySCHISMForcing:
        # TODO:
        raise NotImplementedError('This forcing is not supported for SCHISM')

    @classmethod
    def from_pyschism(cls, forcing: PySCHISMForcing) -> 'ForcingJSON':
        # TODO:
        raise NotImplementedError('This forcing is not supported for SCHISM')


class ATMESHForcingJSON(WindForcingJSON, FileForcingJSON, TimestepForcingJSON, NEMSCapJSON):
    """
    atmospheric mesh (ATMESH) configuration in ``configure_atmesh.json``

    stores NWS parameter, forcing read interval, and optionally NEMS parameters

    .. code-block:: python

        configuration = ATMESHForcingJSON(
            resource='Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc',
            nws=17,
            interval=timedelta(hours=1),
        )

    """

    name = 'ATMESH'
    default_filename = f'configure_atmesh.json'
    default_nws = 17
    default_interval = timedelta(hours=1)
    default_processors = 1

    def __init__(
        self,
        resource: PathLike,
        nws: int = None,
        interval: timedelta = None,
        processors: int = None,
        nems_parameters: Dict[str, str] = None,
        **kwargs,
    ):
        WindForcingJSON.__init__(self, nws=nws, **kwargs)
        FileForcingJSON.__init__(self, resource=resource, **kwargs)
        NEMSCapJSON.__init__(
            self, processors=processors, nems_parameters=nems_parameters, **kwargs
        )
        TimestepForcingJSON.__init__(self, interval=interval, **kwargs)

    @property
    def adcircpy_forcing(self) -> ADCIRCPyForcing:
        return ADCIRCPyAtmosphericMeshForcing(
            filename=self['resource'],
            nws=self['nws'],
            interval_seconds=self['interval'] / timedelta(seconds=1),
        )

    @classmethod
    def from_adcircpy(cls, forcing: ADCIRCPyAtmosphericMeshForcing) -> 'ATMESHForcingJSON':
        return cls(resource=forcing.filename, nws=forcing.NWS, interval=forcing.interval)

    @property
    def pyschism_forcing(self) -> PySCHISMForcing:
        # TODO:
        raise NotImplementedError('This forcing is not supported for SCHISM')

    @classmethod
    def from_pyschism(cls, forcing: PySCHISMForcing) -> 'ForcingJSON':
        # TODO:
        raise NotImplementedError('This forcing is not supported for SCHISM')

    @property
    def nemspy_entry(self) -> AtmosphericForcingEntry:
        return AtmosphericForcingEntry(
            filename=self['resource'], processors=self['processors'], **self['nems_parameters']
        )


class WaveForcingJSON(ForcingJSON, ABC):
    """
    abstraction of a wave forcing configuration

    stores NRS parameter (hundredths place of NWS parameter in ``fort.15``)
    """

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
    """
    WaveWatch III output file configuration in ``configure_ww3data.json``

    stores NRS parameter, forcing read interval, and optionally NEMS parameters

    .. code-block:: python

        configuration = WW3DATAForcingJSON(
            resource='ww3.HWRF.NOV2018.2012_sxy.nc',
            nrs=5,
            interval=timedelta(hours=1),
        )

    """

    name = 'WW3DATA'
    default_filename = f'configure_ww3data.json'
    default_nrs = 5
    default_interval = timedelta(hours=1)
    default_processors = 1

    def __init__(
        self,
        resource: PathLike,
        nrs: int = None,
        interval: timedelta = None,
        processors: int = None,
        nems_parameters: Dict[str, str] = None,
        **kwargs,
    ):
        WaveForcingJSON.__init__(self, nrs=nrs, **kwargs)
        FileForcingJSON.__init__(self, resource=resource, **kwargs)
        NEMSCapJSON.__init__(
            self, processors=processors, nems_parameters=nems_parameters, **kwargs
        )
        TimestepForcingJSON.__init__(self, interval=interval, **kwargs)

    @property
    def adcircpy_forcing(self) -> ADCIRCPyForcing:
        return ADCIRCPyWaveWatch3DataForcing(
            filename=self['resource'], nrs=self['nrs'], interval_seconds=self['interval'],
        )

    @classmethod
    def from_adcircpy(cls, forcing: ADCIRCPyWaveWatch3DataForcing) -> 'WW3DATAForcingJSON':
        return cls(resource=forcing.filename, nrs=forcing.NRS, interval=forcing.interval)

    @property
    def pyschism_forcing(self) -> PySCHISMForcing:
        # TODO:
        raise NotImplementedError('This forcing is not supported for SCHISM')

    @classmethod
    def from_pyschism(cls, forcing: PySCHISMForcing) -> 'ForcingJSON':
        # TODO:
        raise NotImplementedError('This forcing is not supported for SCHISM')

    @property
    def nemspy_entry(self) -> WaveWatch3ForcingEntry:
        return WaveWatch3ForcingEntry(
            filename=self['resource'], processors=self['processors'], **self['nems_parameters']
        )


class HydrologyForcingJSON(ForcingJSON, ABC):
    """
    abstraction of a hydrology forcing configuration
    """

    field_types = {}

    def __init__(self, **kwargs):
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(HydrologyForcingJSON.field_types)

        ForcingJSON.__init__(self, **kwargs)


class NationalWaterModelFocringJSON(HydrologyForcingJSON, FileForcingJSON):
    """
    National water model file configuration in ``configure_nwm.json``

    .. code-block:: python
        TODO

    """

    name = 'NWM'
    default_filename = f'configure_nwm.json'
    default_aggregation_radius = None
    defaul_cache = False
    field_types = {
        'aggregation_radius': float,
        'cache': bool,
    }

    def __init__(
        self, resource: PathLike, **kwargs,
    ):
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(NationalWaterModelFocringJSON.field_types)

        HydrologyForcingJSON.__init__(self, **kwargs)
        FileForcingJSON.__init__(self, resource=resource, **kwargs)

    @property
    def adcircpy_forcing(self) -> None:
        raise NotImplementedError('ADCIRC does NOT support NWM forcing!')

    @classmethod
    def from_adcircpy(cls, forcing: None) -> 'None':
        raise NotImplementedError('ADCIRC does NOT support NWM forcing!')

    @property
    def pyschism_forcing(self) -> PySCHISMNWM:
        return PySCHISMNWM(
            aggregation_radius=self['aggregation_radius'],
            nwm_file=self['resource'],
            cache=self['cache'],
        )

    @classmethod
    def from_pyschism(cls, forcing: PySCHISMNWM) -> 'ForcingJSON':
        return cls(
            resource=forcing.nwm_file,
            aggregation_radius=forcing.aggregation_radius,
            cache=forcing.cache,
        )

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
import sys
from typing import Any, Dict, List

from adcircpy import Tides
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.tides import HAMTIDE
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds import BestTrackForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from adcircpy.forcing.winds.owi import OwiForcing
from nemspy.model import AtmosphericForcingEntry, WaveWatch3ForcingEntry
from pandas import DataFrame

from coupledmodeldriver.configure.base import AttributeJSON, ConfigurationJSON, NEMSCapJSON
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
    """
    abstraction of a forcing configuration
    """

    @property
    @abstractmethod
    def adcircpy_forcing(self) -> Forcing:
        """
        create an ADCIRCpy forcing object with values from this configuration
        """

        raise NotImplementedError

    def to_adcircpy(self) -> Forcing:
        return self.adcircpy_forcing

    @classmethod
    @abstractmethod
    def from_adcircpy(cls, forcing: Forcing) -> 'ForcingJSON':
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


class TidalForcingJSON(FileForcingJSON):
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
        tidal_source: TidalSource = TidalSource.TPXO,
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
        'fort22_filename': Path,
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
    def adcircpy_forcing(self) -> BestTrackForcing:
        if self['fort22_filename'] is not None:
            forcing = BestTrackForcing.from_fort22(
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
            forcing = BestTrackForcing(
                storm=self.__dataframe,
                nws=self['nws'],
                interval_seconds=self['interval'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
        elif self['nhc_code'] is not None:
            forcing = BestTrackForcing(
                storm=self['nhc_code'],
                nws=self['nws'],
                interval_seconds=self['interval'],
                start_date=self['start_date'],
                end_date=self['end_date'],
            )
        else:
            raise ValueError(
                f'could not create `{BestTrackForcing.__name__}` object from given information'
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
    def from_adcircpy(cls, forcing: BestTrackForcing) -> 'BestTrackForcingJSON':
        return cls(
            nhc_code=forcing.nhc_code,
            nws=forcing.NWS,
            interval=forcing.interval,
            start_date=forcing.start_date,
            end_date=forcing.end_date,
            fort22_filename=forcing.filename,
            dataframe=forcing.data,
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
        forcing = BestTrackForcing.from_fort22(
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
    def adcircpy_forcing(self) -> OwiForcing:
        return OwiForcing(interval_seconds=self['interval'] / timedelta(seconds=1))

    @classmethod
    def from_adcircpy(cls, forcing: OwiForcing) -> 'OWIForcingJSON':
        return cls(interval=timedelta(seconds=forcing.interval))


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
    def adcircpy_forcing(self) -> Forcing:
        return AtmosphericMeshForcing(
            filename=self['resource'],
            nws=self['nws'],
            interval_seconds=self['interval'] / timedelta(seconds=1),
        )

    @classmethod
    def from_adcircpy(cls, forcing: AtmosphericMeshForcing) -> 'ATMESHForcingJSON':
        return cls(resource=forcing.filename, nws=forcing.NWS, interval=forcing.interval)

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
    def adcircpy_forcing(self) -> Forcing:
        return WaveWatch3DataForcing(
            filename=self['resource'], nrs=self['nrs'], interval_seconds=self['interval'],
        )

    @classmethod
    def from_adcircpy(cls, forcing: WaveWatch3DataForcing) -> 'WW3DATAForcingJSON':
        return cls(resource=forcing.filename, nrs=forcing.NRS, interval=forcing.interval)

    @property
    def nemspy_entry(self) -> WaveWatch3ForcingEntry:
        return WaveWatch3ForcingEntry(
            filename=self['resource'], processors=self['processors'], **self['nems_parameters']
        )

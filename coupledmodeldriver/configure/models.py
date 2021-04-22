from abc import ABC
from enum import Enum

from coupledmodeldriver.configure.base import ConfigurationJSON


class Model(Enum):
    ADCIRC = 'ADCIRC'
    TidalForcing = 'Tides'
    ATMESH = 'ATMESH'
    WW3DATA = 'WW3DATA'


class ModelJSON(ConfigurationJSON, ABC):
    def __init__(self, model: Model, **kwargs):
        if not isinstance(model, Model):
            model = Model[str(model).lower()]

        ConfigurationJSON.__init__(self, **kwargs)

        self.model = model

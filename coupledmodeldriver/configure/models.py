from abc import ABC
from enum import Enum

from coupledmodeldriver.configure.base import ConfigurationJSON


class Model(Enum):
    """
    model information -> class name
    """

    ADCIRC = 'ADCIRC'
    SCHISM = 'SCHISM'
    TidalForcing = 'Tides'
    ATMESH = 'ATMESH'
    WW3DATA = 'WW3DATA'


class ModelJSON(ConfigurationJSON, ABC):
    """
    abstraction of a model configuration

    stores model information
    """

    def __init__(self, model: Model, **kwargs):
        if not isinstance(model, Model):
            model = Model[str(model).lower()]

        ConfigurationJSON.__init__(self, **kwargs)

        self.model = model

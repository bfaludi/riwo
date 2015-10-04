from .. import (
    Reader as AbstractReader,
)
from ..utils import *

class Reader(AbstractReader):
    # Iterable
    def get_iterable_data(self):
        return to_iterable(self.resource)
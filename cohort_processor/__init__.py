# cohort_processor/__init__.py

from .cohort_processor import CohortGenerator
from . import config
from . import utils
from . import impl

__all__ = [
    "CohortGenerator",
    "config",
    "utils",
    "impl",
]

__version__ = "0.1.0"

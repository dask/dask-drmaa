from .core import DRMAACluster, get_session
from .sge import SGECluster
from .adaptive import Adaptive

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

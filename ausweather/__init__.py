from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass


from ausweather.core import *
from ausweather.database import *
from ausweather.bom import *
from ausweather.silo import *
from ausweather.charts import *

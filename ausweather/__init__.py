from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

from datetime import datetime, date, timedelta
import re

import pandas as pd

from ausweather.database import *
from ausweather.bom import *
from ausweather.silo import *
from ausweather.charts import *


def fetch_bom_station_from_silo(
    bom_station, email, query_from="1950-01-01", only_use_complete_years=False
):
    query_from = pd.Timestamp(query_from)
    rf_data = silo_alldata(
        bom_station,
        email,
        start=int(query_from.strftime("%Y%m%d")),
        return_comments=True,
    )
    df = rf_data["df"]
    rex = re.compile(r'\W+')

    title = ""
    for line in rf_data['comments'].splitlines():
        if 'Patched Point data for station' in line:
            title = ' '.join(rex.sub(' ', line.split(':', 1)[1]).split()[:2])
    title += f' (fetched from SILO on {datetime.now()})'

    if only_use_complete_years:
        df = df.groupby([df.Date.dt.year]).filter(lambda x: len(x) >= 365)
    rf_annual = df.groupby([df.Date.dt.year]).Rain.sum()
    rf_annual_srn = df.groupby([df.Date.dt.year]).Srn.agg(
        [
            lambda x: len(x[x.isin([25, 35, 75])]) / 365.25 * 100,
            lambda x: len(x[x == 15]) / 365.25 * 100,
            #         lambda x: len(x[x == 0]) / len(x) * 100,
        ]
    )
    rf_annual_srn.columns = ["Interpolated", "Deaccumulated"]

    return {
        "silo_returned": rf_data,
        'station_no': bom_station,
        'station_name': title.split()[1],
        "title": title,
        "df": df,
        "annual": rf_annual,
        "srn": rf_annual_srn,
    }

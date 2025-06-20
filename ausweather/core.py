import logging
from datetime import datetime
import re

import pandas as pd

from ausweather.silo import silo_alldata


logger = logging.getLogger(__name__)

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
    rex = re.compile(r"\W+")

    title = ""
    name = ""
    for line in rf_data["comments"].splitlines():
        if "Patched Point data for station" in line:
            colon_parts = line.split(":")
            title = colon_parts[1].replace("Lat", "").strip()
            name = title.replace(str(bom_station), "").strip()
            break
    title += f" (fetched from SILO on {datetime.now()})"
    print(f"station #: {bom_station} name: {name} title: {title}")

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
        "station_no": bom_station,
        "station_name": name,
        "title": title,
        "df": df,
        "annual": rf_annual,
        "srn": rf_annual_srn,
    }



INTERPOLATION_CODES = {
    0: "observed",
    15: "deaccumulated",
    25: "interpolated",
    26: "synthetic",
    35: "interpolated_by_anomaly",
    42: "satellite_estimate",
    75: "interpolated_by_long_term_averages",
}

class RainfallStationData:
    """Rainfall station data.

    You should initialise this by using any of these class methods:

    - :meth:`wrap_technote.RainfallStationData.from_bom_via_silo`
    - :meth:`wrap_technote.RainfallStationData.from_aquarius`
    - :meth:`wrap_technote.RainfallStationData.from_wrap_report`
    - :meth:`wrap_technote.RainfallStationData.from_data`

    e.g.

    .. code-block::

        >>> rf = RainfallStationData.from_bom_via_silo('18017')

    You can then access data via the ``rf.daily``, ``rf.calendar``, or
    ``rf.financial`` attributes.

    Args:
        station_id (str): station ID
        exclude_incomplete_years (bool): only show complete years

    """

    def __init__(self, station_id, exclude_incomplete_years=False):
        self.station_id = str(station_id)
        self.exclude_incomplete_years = False

    @property
    def exclude_incomplete_years(self):
        return self.__exclude_incomplete_years

    @exclude_incomplete_years.setter
    def exclude_incomplete_years(self, value):
        if value:
            self.__exclude_incomplete_years = True
        else:
            self.__exclude_incomplete_years = False

    @classmethod
    def from_bom_via_silo(cls, station_id, data_start=None):
        """Create from BoM data (via SILO).

        Args:
            station_id (str): BoM Station ID
            data_start (pd.Timestamp): date to download data from, default 1/1/1950

        Returns:
            :class:`wrap_technote.RainfallStationData`

        Note that this will download the data afresh from the SILO website.

        """
        if data_start is None:
            data_start = pd.Timestamp("1950-01-01")
        self = cls(station_id)
        self.df = download_bom_rainfall(station_id, data_start)
        return self

    @classmethod
    def from_aquarius(cls, station_id, data_start=None):
        """Create from Aquarius Timeseries data (for South Australia only)

        Args:
            station_id (str): AQTS LocationIdentifier.
            data_start (pd.Timestamp): date to download data from, default 1/1/1950

        Returns:
            :class:`wrap_technote.RainfallStationData`

        Note that this will download the data afresh from AQTS.

        """
        if data_start is None:
            data_start = pd.Timestamp("1950-01-01")
        self = cls(station_id)
        self.df = download_aquarius_rainfall(station_id, data_start)

        return self

    @property
    def daily(self):
        """Daily rainfall data.

        Returns:
            :class:`pandas.DataFrame`: dataframe with columns:

            - date (pd.Timestamp)
            - rainfall (float)
            - interpolated_code (int)
            - quality (int)
            - year (int)
            - dayofyear (int)
            - finyear (str)
            - station_id (str)

        """
        df = self.df.assign(station_id=self.station_id)
        return df

    @property
    def calendar(self):
        df = self.groupby("year").assign(station_id=self.station_id)
        df.insert(1, "start_date", [pd.Timestamp(f"{y}-01-01") for y in df.year])
        if self.exclude_incomplete_years:
            missing = find_missing_days(
                self.daily, dt_col="date", year_type="calendar", value_col="rainfall"
            )
            complete = missing[missing == 0]
            df = df[df.year.isin(complete.index.values)]
        return df

    @property
    def financial(self):
        df = self.groupby("finyear").assign(station_id=self.station_id)
        df.insert(1, "start_date", [pd.Timestamp(f"{y[:4]}-07-01") for y in df.finyear])
        if self.exclude_incomplete_years:
            missing = find_missing_days(
                self.daily, dt_col="date", year_type="financial", value_col="rainfall"
            )
            complete = missing[missing == 0]
            df = df[df.finyear.isin(complete.index.values)]
        return df

    def groupby(self, grouping_column):
        """Group daily rainfall by either calendar or financial year.

        Args:
            grouping_column (str): either 'year' or 'finyear'

        Returns:
            :class:`pandas.DataFrame`: dataframe with these columns:

            - year or finyear (str)
            - rainfall (float): rainfall in mm
            - rainfall_count (int): number of days with data
            - interpolated_count (int): number of days with interpolated_code != 0
            - quality_count (int): number of days with non-null quality code.

        """
        assert grouping_column in self.df.columns
        return self.df.groupby(grouping_column, as_index=False).agg(
            rainfall=("rainfall", "sum"),
            rainfall_count=("rainfall", "count"),
            interpolated_count=(
                "interpolated_code",
                lambda x: len([xi for xi in x if xi != 0]),
            ),
            quality_count=(
                "quality",
                lambda x: len([xi for xi in x if not pd.isnull(xi)]),
            ),
        )


def download_bom_rainfall(station_id, data_start, email):
    """Download BoM rainfall data from SILO.

    Args:
        station_id (int or str): BoM station ID
        data_start (pd.Timestamp): date to download data from
        email (str): put your email in here

    Returns:
        dict: Has four keys: 'df', 'annual', 'srn', and 'wateruse_year'

    This function uses :func:`fetch_bom_station_from_silo`
    in the background.

    """
    station_id = int(f"{float(station_id):.0f}")
    logger.debug(f"Downloading {station_id} from {data_start}")

    data = fetch_bom_station_from_silo(
        station_id, email, data_start
    )

    df = data["df"]
    df["date"] = pd.to_datetime(df["Date"])
    # df["Grade"] = pd.NA
    df["Grade"] = 1
    df = df.rename(
        columns={
            "Rain": "rainfall",
            "Srn": "interpolated_code",
            "Grade": "quality",
        }
    )
    df["year"] = df["date"].dt.year
    df["dayofyear"] = df["date"].dt.dayofyear
    df["finyear"] = [date_to_wateruseyear(d) for d in df["date"]]
    df["interpolated_desc"] = df.interpolated_code.map(INTERPOLATION_CODES)

    cols = [
        "date",
        "rainfall",
        "interpolated_code",
        "interpolated_desc",
        "quality",
        "year",
        "dayofyear",
        "finyear",
    ]
    return df[cols]


def download_aquarius_rainfall(station_id, data_start=None):
    """Download rainfall data from DEW's Aquarius Web Portal website.

    Args:
        station_id (int or str): Aquarius location ID
        data_start (pd.Timestamp): date to download data from (optional)

    Returns:
        dict:  with four keys: 'df', 'annual', 'srn', and 'wateruse_year'

    """
    if data_start is None:
        data_start = pd.Timestamp("1800-01-01")

    logger.debug(f"Downloading {station_id} from {data_start} from Aquarius")

    url = f"https://water.data.sa.gov.au/Export/BulkExport?DateRange=EntirePeriodOfRecord&TimeZone=9.5&Calendar=CALENDARYEAR&Interval=Daily&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=True&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Rainfall.Best%20Available--Continuous%40{station_id}&Datasets[0].Calculation=Aggregate&Datasets[0].UnitId=89"
    resp = requests.get(url, verify=False)

    buffer = io.StringIO()
    buffer.write(resp.text)
    buffer.seek(0)
    i = 0
    for line in buffer:
        if i == 2:
            station_name = line.strip("\n").strip(",").strip()
        break

    df = pd.read_csv(
        buffer, skiprows=5, names=["Date", "end_timestamp", "Rain", "Grade"]
    )

    df = df.rename(columns={"Date": "date", "Rain": "rainfall", "Grade": "quality"})
    df["interpolated_code"] = 0
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["dayofyear"] = df["date"].dt.dayofyear
    df["finyear"] = [date_to_wateruseyear(d) for d in df["date"]]
    df["interpolated_desc"] = df.interpolated_code.map(INTERPOLATION_CODES)

    cols = [
        "date",
        "rainfall",
        "interpolated_code",
        "interpolated_desc",
        "quality",
        "year",
        "dayofyear",
        "finyear",
    ]
    return df[cols]

def get_spanning_dates(
    date_series: pd.Series, year_type: str = "calendar"
) -> pd.DatetimeIndex:
    """Given a pandas Series of datetimes, return a DatetimeIndex which
    spans all the days within the range of years that *date_series*
    spans.

    Args:
        date_series (pd.Series): a sequence of dates
        year_type (str): either 'calendar' or 'financial' - defines what
            'year' means

    Returns:
        pd.DatetimeIndex: pandas DateTimeIndex of contiguous dates.

    e.g.

    .. code-block:: python

        >>> dates = pd.Series([pd.Timestamp(x) for x in ["2018-02-02"]])
        >>> print(dates)
        0   2018-02-02
        dtype: datetime64[ns]
        >>> get_spanning_dates(dates, year_type="calendar")
        DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03', '2018-01-04',
                       '2018-01-05', '2018-01-06', '2018-01-07', '2018-01-08',
                       '2018-01-09', '2018-01-10',
                       ...
                       '2018-12-22', '2018-12-23', '2018-12-24', '2018-12-25',
                       '2018-12-26', '2018-12-27', '2018-12-28', '2018-12-29',
                       '2018-12-30', '2018-12-31'],
                      dtype='datetime64[ns]', length=365, freq='D')
        >>> get_spanning_dates(dates, year_type="financial")
        DatetimeIndex(['2017-07-01', '2017-07-02', '2017-07-03', '2017-07-04',
                       '2017-07-05', '2017-07-06', '2017-07-07', '2017-07-08',
                       '2017-07-09', '2017-07-10',
                       ...
                       '2018-06-21', '2018-06-22', '2018-06-23', '2018-06-24',
                       '2018-06-25', '2018-06-26', '2018-06-27', '2018-06-28',
                       '2018-06-29', '2018-06-30'],
                      dtype='datetime64[ns]', length=365, freq='D')

    """
    if year_type == "calendar":
        year_min = date_series.min().year
        year_max = date_series.max().year
        logger.debug(f"year_min {year_min}, year_max {year_max}")
        start_day = pd.Timestamp(year=year_min, month=1, day=1)
        finish_day = pd.Timestamp(year=year_max, month=12, day=31)
    elif year_type == "financial":
        first_day = date_series.min()
        last_day = date_series.max()
        if first_day.month >= 7:
            year_min = first_day.year
        else:
            year_min = first_day.year - 1
        if last_day.month >= 7:
            year_max = last_day.year + 1
        else:
            year_max = last_day.year
        logger.debug(f"year_min {year_min}, year_max {year_max}")
        start_day = pd.Timestamp(year=int(year_min), month=7, day=1)
        finish_day = pd.Timestamp(year=year_max, month=6, day=30)
    else:
        raise KeyError("year_type must be either 'calendar' or 'financial'")
    logger.debug(f"start_day: {start_day}, finish_day: {finish_day}")
    return pd.date_range(start_day, finish_day)


def find_missing_days(
    df: pd.DataFrame,
    dt_col: str = "timestamp",
    year_type: str = "financial",
    value_col: str = "value",
) -> pd.Series:
    """Find the number of missing days in a year from a daily dataset.

    Args:
        df (pd.DataFrame): table including dates and values
        dt_col (str): name of column in *df* which contains datetimes.
        year_type (str): what does "year" mean? either "financial" or
            "calendar"
        value_col (str): name of column in *df* which contains the data itself

    See :func:`wrap_technote.get_spanning_dates` for more information on
    the keyword argument *year_type*.

    Returns:
        pd.Series: pandas Series with the relevant *year_type* values as the index,
        and the number of missing days within each year.

    """
    all_days = get_spanning_dates(df[dt_col], year_type=year_type)
    day_is_missing = (
        df.set_index(dt_col).reindex(all_days)[value_col].isnull().reset_index()
    )
    day_is_missing["year"] = day_is_missing["index"].dt.year
    day_is_missing["finyear"] = [
        date_to_wateruseyear(d) for d in day_is_missing["index"]
    ]
    year_type_col = {"financial": "finyear", "calendar": "year"}[year_type]
    missing_days = day_is_missing.groupby([day_is_missing[year_type_col]])[value_col].sum()
    return missing_days

def date_to_wateruseyear(d):
    """Convert :class:`datetime.date` to water-use year as string.

    e.g. date(2016, 5, 3) -> "2015-16", while date(2016, 11, 1) -> "2016-17"

    Args:
        d (:class:`datetime.date`): date

    Returns:
        str: financial/water-use year e.g. "2019-20"

    """
    year = d.year
    if d.month >= 7:
        return "{}-{}".format(year, str(year + 1)[2:])
    else:
        return "{}-{}".format(year - 1, str(year)[2:])

def reduce_daily_to_monthly(
    daily_df, dt_col="Date", year_col="wu_year", value_col="Rain"
):
    """Reduce daily rainfall totals into monthly totals per year.

    Args:
        daily_df (pandas DataFrame): daily rainfall data
        dt_col (str): column of *daily_df* with the date as a datetime
        year_col (str): column of *daily_df* with a value to group as
            years. It could be the year itself i.e.
            `daily_df[dt_col].dt.year` or it could be the financial year
        value_col (str): column with the rainfall total

    Returns:
        pd.DataFrame: a dataframe with columns *year_col*, "month", and *value_col*.

    """
    grouper = daily_df.groupby([daily_df[year_col], daily_df[dt_col].dt.month])
    sums = grouper[value_col].sum()
    return sums.reset_index().rename(columns={dt_col: "month"})
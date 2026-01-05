import logging
from datetime import datetime
import re
import io

import pandas as pd
import requests
from scipy import stats

from ausweather.silo import silo_alldata, get_silo_station_list
from ausweather.bom import parse_bom_rainfall_station_list

SA_BOM_RAINFALL_LIST = parse_bom_rainfall_station_list()

logger = logging.getLogger(__name__)


INTERPOLATION_CODES = {
    0: "observed",
    15: "deaccumulated",
    25: "interpolated",
    26: "synthetic",
    35: "interpolated_by_anomaly",
    42: "satellite_estimate",
    75: "interpolated_by_long_term_averages",
}


def get_sa_rainfall_site_list():
    """Get a list of SA rainfall stations available via SILO.

    Uses :func:`get_silo_station_list` and :func:`parse_bom_rainfall_station_list`.

    """
    silo_list = get_silo_station_list()
    sa_bom_list = parse_bom_rainfall_station_list()
    df = pd.merge(
        silo_list,
        sa_bom_list[["station_id", "start", "end", "aws"]],
        on="station_id",
        how="inner",
    )
    df["station_id"] = df.station_id.astype(str)
    df["current"] = df["end"].apply(
        lambda end: (pd.Timestamp(datetime.now()) - pd.Timestamp(end))
        < pd.Timedelta(days=120)
    )
    df["total_span_yrs"] = df.apply(
        lambda row: (row.end - row.start).days / 365.25, axis=1
    )
    return df


class RainfallStationData:
    """Rainfall station data.

    You should initialise this by using any of these class methods:

    - :meth:`ausweather.RainfallStationData.from_bom_via_silo`
    - :meth:`ausweather.RainfallStationData.from_aquarius`

    e.g.

    .. code-block::

        >>> rf = RainfallStationData.from_bom_via_silo('18017', 'your@email.com')

    You can then access data via the ``rf.daily``, ``rf.calendar``, or
    ``rf.financial`` attributes.

    Args:
        station_id (str): station ID
        exclude_incomplete_years (bool): only show complete years

    """

    def __init__(self, station_id, exclude_incomplete_years=False):
        self.station_id = str(station_id)
        self.exclude_incomplete_years = exclude_incomplete_years

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
    def from_bom_via_silo(
        cls, station_id, email, data_start=None, clip_ends=True, data_end=None, **kwargs
    ):
        """Create from BoM data (via SILO).

        Args:
            station_id (str): BoM Station ID
            email (str): email address, required by SILO API
            data_start (pd.Timestamp): date to download data from, default is to use
                the first observation per the BoM's Weather Station Directory.
            exclude_incomplete_years (bool): only show complete years
            data_end (pd.Timestamp): date to download to. If not provided,
                either the end of data collection at the station will
                be used, or 5 days prior to the current day, whichever is
                earlier. Remember that data is being obtained from SILO
                which will interpolate values if necessary, so if you
                want data beyond the date at which the station closed
                you will need to specify the end date.
            clip_ends (bool): ...

        Returns:
            :class:`ausweather.RainfallStationData`

        Note that this will download the data afresh from the SILO website.

        """
        self = cls(station_id, **kwargs)
        self.df = download_bom_rainfall(
            station_id,
            email,
            data_start=data_start,
            clip_ends=clip_ends,
            data_end=data_end,
        )
        self.df["month"] = self.df.date.dt.month
        return self

    @classmethod
    def from_aquarius(cls, station_id, data_start=None, **kwargs):
        """Create from Aquarius TS data (for South Australia only)

        Args:
            station_id (str): AQTS LocationIdentifier.
            data_start (pd.Timestamp): date to download data from, default 1/1/1950
            exclude_incomplete_years (bool): only show complete years

        Returns:
            :class:`ausweather.RainfallStationData`

        Note that this will download the data afresh from water.data.sa.gov.au

        """
        if data_start is None:
            data_start = pd.Timestamp("1950-01-01")
        self = cls(station_id, **kwargs)
        self.df = download_aquarius_rainfall(station_id, data_start)
        self.df["month"] = self.df.date.dt.month
        return self

    @classmethod
    def from_data(cls, station_id, df, **kwargs):
        """Create from daily data.

        Args:
            station_id (str): station ID
            df (pd.DataFrame): data, see source for required columns

        """
        self = cls(station_id, **kwargs)
        logger.debug(f"creating from_data df=\n{df}")
        self.df = df
        self.df["month"] = self.df.date.dt.month
        return self

    @property
    def daily(self):
        """Daily rainfall data.

        Returns:
            :class:`pandas.DataFrame`: dataframe with columns:

            - year (int)
            - finyear (str)
            - month (int)
            - date (pd.Timestamp)
            - dayofyear (int)
            - rainfall (float)
            - interpolated_code (int)
            - quality (int)
            - station_id (str)

        """
        df = self.df.assign(station_id=self.station_id)
        for col in df.columns:
            if col.startswith("date"):
                df[col] = pd.to_datetime(df[col])
        df["interpolated_desc"] = df.interpolated_code.map(INTERPOLATION_CODES)
        df["rainfall"] = df.rainfall.round(decimals=1)
        # cols = ["year", "finyear", "month", "date", "dayofyear", "rainfall", "interpolated_code", "quality", "station_id"]
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
        return df.reset_index()

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
        return df.reset_index()

    @property
    def month(self):
        df = self.groupby(["year", "month"]).assign(station_id=self.station_id)
        df.insert(
            2,
            "start_date",
            [
                pd.Timestamp(f"{row.year}-{row.month:02.0f}-01")
                for idx, row in df.iterrows()
            ],
        )
        df.insert(2, "year_month", df.start_date.dt.strftime("%Y-%m"))
        return df.reset_index()

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


def annual_stats(
    df, avg_pd_start=None, avg_pd_end=None, dt_col="year", value_col="rainfall"
):
    """Calculate descriptive statistics for e.g. rainfall data.

    Args:
        df (pd.DataFrame): should have data arranged by e.g. year
        avg_pd_start (object): the first e.g. year to use for calculation of statistics
        avg_pd_end (object): the last e.g. year to use for calculation of statistics
        dt_col (str): column containing "year" values i.e. can be anything that can be
            ordered, and if avg_pd_start and avg_pd_end are provided, they should be
            values from this column or that make sense for this column in terms of
            ordering.
        value_col (str): column containing data to calculate statistics for.

    Returns:
        dictionary containing keys:
        - mean
        - median
        - min
        - max
        - pct5
        - pct25
        - pct75
        - pct95
        - percentile: a function which when passed a float will return the percentile
        it falls in according to the period of data used for these statistics
        - data: array of the data being used

    """
    if not avg_pd_start:
        avg_pd_start = df[dt_col].sort_values().iloc[0]
    if not avg_pd_end:
        avg_pd_end = df[dt_col].sort_values().iloc[-1]
    avg_series = df.loc[
        (df[dt_col] >= avg_pd_start) & (df[dt_col] <= avg_pd_end), value_col
    ]
    avg_values = avg_series.agg(
        {
            "mean": "mean",
            "median": "median",
            "min": "min",
            "max": "max",
            "pct5": lambda s: s.quantile(0.05),
            "pct25": lambda s: s.quantile(0.25),
            "pct75": lambda s: s.quantile(0.75),
            "pct95": lambda s: s.quantile(0.95),
        }
    ).to_dict()
    avg_values["percentile"] = lambda value: float(
        stats.percentileofscore(avg_series.values, value, kind="mean")
    )
    return avg_values


def monthly_stats(
    df,
    avg_pd_start=None,
    avg_pd_end=None,
    year_col="year",
    month_col="month",
    value_col="rainfall",
):
    """Calculate descriptive statistics for e.g. rainfall data.

    Args:
        df (pd.DataFrame): should have data arranged by year in one column and month number in another
        avg_pd_start (tuple): the first year and month to use for calculation of statistics e.g. (1960, 1) for January 1960
        avg_pd_end (tuple): the last year and month to use for calculation of statistics
        year_col (str): column containing years as integers
        month_col (str): column containing months as integers
        value_col (str): column containing data to calculate statistics for.

    Returns:
        pandas DataFrame: the index is the month as integer and the columns are
        - mean
        - median
        - min
        - max
        - pct5
        - pct25
        - pct75
        - pct95
        - percentile: a function which when passed a float will return the percentile
        it falls in according to the period of data used for these statistics

    """
    logger.debug(f"passed avg_pd_start={avg_pd_start} avg_pd_end={avg_pd_end}")
    df = df.sort_values([year_col, month_col]).reset_index()

    if not avg_pd_start:
        avg_pd_start = tuple([int(x) for x in df.iloc[0][["year", "month"]].values])
    if not avg_pd_end:
        avg_pd_end = tuple([int(x) for x in df.iloc[-1][["year", "month"]].values])

    logger.debug(
        f"After argument checks. avg_pd_start = {avg_pd_start} avg_pd_end = {avg_pd_end}"
    )
    avg_pd_start_idx = df.loc[
        (df[year_col] == avg_pd_start[0]) & (df[month_col] == avg_pd_start[1])
    ].index[0]
    avg_pd_end_idx = df.loc[
        (df[year_col] == avg_pd_end[0]) & (df[month_col] == avg_pd_end[1])
    ].index[0]

    avg_pd_df = df.iloc[avg_pd_start_idx:avg_pd_end_idx]
    monthly_data = avg_pd_df.set_index("month")[value_col]
    avg_df = avg_pd_df.groupby("month")[value_col].agg(
        **{
            "mean": "mean",
            "median": "median",
            "min": "min",
            "max": "max",
            "pct5": lambda s: s.quantile(0.05),
            "pct25": lambda s: s.quantile(0.25),
            "pct75": lambda s: s.quantile(0.75),
            "pct95": lambda s: s.quantile(0.95),
        }
    )
    for month, row in avg_df.iterrows():
        func = lambda value: float(
            stats.percentileofscore(monthly_data.loc[month], value, kind="mean")
        )
        avg_df.loc[month, "percentile"] = func

    return avg_df


def calculate_deviations(df, stdict, est_col="mean", value_col="rainfall"):
    """Calculate deviations from average statistic.

    Args:
        df (pandas.DataFrame): should have data arranged by e.g. year
        stdict (dict): the return value from :func:`ausweather.calculate_statistics`
        est_col (str): the central estimate column from ``stdict`` e.g. "mean"
        value_col (str): the data value column from ``df`` e.g. "rainfall"

    Returns:
        A copy of ``df`` (pandas.DataFrame) with new columns:
        - deviation - in the same units as value_col
        - deviation_pct - as a percent
        - percentile - the percentile of value_col according to the
          period used to calculate the statistics (stdict)

    """
    pdf = df.copy()
    pdf["deviation"] = pdf[value_col] - stdict[est_col]
    pdf["deviation_pct"] = pdf["deviation"] / stdict[est_col] * 100
    pdf["percentile"] = pdf[value_col].apply(
        lambda value: np.round(stdict["percentile"](value), decimals=1)
    )
    return pdf


def download_bom_rainfall(
    station_id, email, data_start=None, clip_ends=True, data_end=None
):
    """Download BoM rainfall data from SILO.

    Args:
        station_id (int or str): BoM station ID
        email (str): put your email in here
        data_start (pd.Timestamp): date to download data from. If
            not provided, will use the first observation.
        data_end (pd.Timestamp): date to download to. If not provided,
            either the end of data collection at the station will
            be used, or 5 days prior to the current day, whichever is
            earlier. Remember that data is being obtained from SILO
            which will interpolate values if necessary, so if you
            want data beyond the date at which the station closed
            you will need to specify the end date.
        clip_ends (bool): if True, restrict the dataset to a period
            between the first instance of observed data and the last
            instance. This means any interpolated periods within
            the station's history are preserved, but any interpolated
            periods before the station opened and after the station
            closed are removed automatically. See note above - you
            may need to manually set this to False depending on the
            intent of your query.

    Returns:
        dict: Has four keys: 'df', 'annual', 'srn', and 'wateruse_year'

    This function uses :func:`fetch_bom_station_from_silo`
    in the background.

    """
    station_id = int(f"{float(station_id):.0f}")
    logger.debug(f"Downloading {station_id} from {data_start}")

    data = fetch_bom_station_from_silo(
        station_id, email, query_from=data_start, query_to=data_end
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
    df["finyear"] = [date_to_finyear(d) for d in df["date"]]

    if clip_ends:
        df = df.reset_index()
        df_obs = df[df.interpolated_code == 0]
        first_obs = df_obs.index.values[0]
        last_obs = df_obs.index.values[-1]
        df = df.loc[first_obs:last_obs]

    cols = [
        "date",
        "rainfall",
        "interpolated_code",
        "quality",
        "year",
        "dayofyear",
        "finyear",
    ]
    return df[cols]


def fetch_bom_station_from_silo(
    bom_station, email, query_from=None, query_to=None, only_use_complete_years=False
):
    bom_station = int(bom_station)
    if query_from is None or query_to is None:
        rows = SA_BOM_RAINFALL_LIST.loc[
            SA_BOM_RAINFALL_LIST.station_id == bom_station, ["start", "end"]
        ]
        if len(rows) == 0:
            query_from = query_from
            query_to = query_to
        else:
            row = rows.iloc[0]
            if query_from is None:
                query_from = row.start
            if query_to is None:
                if row.end.month == 12:
                    query_to = f"{row.end.year + 1}-01-01"
                else:
                    query_to = f"{row.end.year}-{row.end.month + 1}-01"
                query_to = pd.Timestamp(query_to)
                if query_to > datetime.now():
                    query_to = datetime.now() - pd.Timedelta(days=5)
                query_to = query_to.strftime("%Y%m%d")

    query_from = pd.Timestamp(query_from)
    if query_from < pd.Timestamp("1889-01-01"):
        query_from = pd.Timestamp("1889-01-01")

    rf_data = silo_alldata(
        bom_station,
        email,
        start=int(query_from.strftime("%Y%m%d")),
        finish=query_to,
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
    df["finyear"] = [date_to_finyear(d) for d in df["date"]]
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

    See :func:`ausweather.get_spanning_dates` for more information on
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
    day_is_missing["finyear"] = [date_to_finyear(d) for d in day_is_missing["index"]]
    year_type_col = {"financial": "finyear", "calendar": "year"}[year_type]
    missing_days = day_is_missing.groupby([day_is_missing[year_type_col]])[
        value_col
    ].sum()
    return missing_days


def date_to_finyear(d):
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


date_to_wateruseyear = date_to_finyear


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

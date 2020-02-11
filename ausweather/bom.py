"""

BoM climate data. Use station code and ``p_nccObsCode`` to find ``p_c``, which is the long negative integer in the last column of this table:

- http://www.bom.gov.au/jsp/ncc/cdio/weatherStationDirectory/d?p_display_type=ajaxStnListing&p_nccObsCode=136&p_stnNum=023107&p_radius=100

Note you can find all stations within a ``p_radius`` (km). Then once you have ``p_c`` you can access the data itself:

- http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_display_type=dailyZippedDataFile&p_stn_num=023343&p_nccObsCode=122&p_c=-108975703
- http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_display_type=dailyZippedDataFile&p_stn_num=023343&p_nccObsCode=136&p_c=-108978592
- http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_display_type=monthlyZippedDataFile&p_stn_num=023343&p_nccObsCode=139&p_c=-108979252

Note daily vs monthly for different reporting frequency ``nccObsCode``s.

Some BoM stations are available from SILO:

https://www.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php?start=19950101&finish=20110110&station=023343&format=alldata&username=<email-address>


"""
import io
import logging

import requests
import pandas as pd


logger = logging.getLogger(__name__)
ncc_obs_codes = {
    136: "Daily total rainfall",
    122: "Daily max temp",
    139: "Monthly total rainfall",
}


def fetch_station_list(ncc_obs_code):
    """Fetch the BoM station list for nccObsCode (for all Australia).

    Args:
        ncc_obs_code (int): the nccObsCode, e.g. 122 for daily max temp.

    Returns:
        pandas DataFrame

    """
    logger.info(f"Fetching BoM station list for AUS obsCode {ncc_obs_code}")
    r = requests.get(
        f"http://www.bom.gov.au/climate/data/lists_by_element/alphaAUS_{ncc_obs_code}.txt"
    )
    n = r.text.count("\n")
    buffer = io.StringIO(r.text)

    # Skip the header and footer
    skiprows = [0, 1, 3] + [n - i for i in range(7)]
    colspecs = [
        (0, 8),
        (8, 49),
        (49, 59),
        (59, 68),
        (68, 77),
        (77, 86),
        (86, 93),
        (93, 97),
        (97, 102),
    ]
    df = pd.read_fwf(buffer, skiprows=skiprows, colspecs=colspecs)
    df["Name"] = df["Name"].astype(str)
    # for month_col in ("Start", "End"):
    #     df[month_col] = [
    #         str(dt) for dt in pd.to_datetime(df[month_col], format="%b %Y")
    #     ]
    df["ncc_obs_code"] = ncc_obs_code
    df["ncc_obs_descr"] = ncc_obs_codes[ncc_obs_code]
    return df


def fetch_c_values(ncc_obs_code, station_code, radius_km=10):
    url = (
        f"http://www.bom.gov.au/jsp/ncc/cdio/weatherStationDirectory"
        f"/d?p_display_type=ajaxStnListing"
        f"&p_nccObsCode={ncc_obs_code}&p_stnNum={station_code}&p_radius={radius_km}"
    )
    r = requests.get(url)
    buffer = io.StringIO(r.text)
    return pd.read_html(buffer)[0].rename(columns={"Unnamed: 10": "c"})

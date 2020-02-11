import io
import logging

import requests
import pandas as pd


logger = logging.getLogger(__name__)
ncc_obs_codes = {136: "Daily total rainfall", 122: "Daily max temp"}


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
    for month_col in ("Start", "End"):
        df[month_col] = [
            str(dt) for dt in pd.to_datetime(df[month_col], format="%b %Y")
        ]
    df["ncc_obs_code"] = ncc_obs_code
    df["ncc_obs_descr"] = ncc_obs_codes[ncc_obs_code]
    return df

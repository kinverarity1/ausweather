"""Some BoM stations are available from SILO:

https://www.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php?start=19950101&finish=20110110&station=023343&format=alldata&username=<email-address>
"""
from datetime import datetime
import io

import pandas as pd
import requests

__all__ = ["silo_alldata"]


def silo_alldata(station_code, email, start=None, finish=None, return_comments=False):
    """Retrieve alldata result from SILO (daily timeseries with temperature,
    rainfall etc).

    Args:
        station_code (int or str): BoM station number
        email (str): used for querying SILO - no need for an account, but you
            need to supply a valid email address.
        start ('auto', datetime, pd.Timestamp, str in YYYYMMDD or int): start of
            time period to retrieve data for
        finish ('auto', datetime, pd.Timestamp, str in YYYYMMDD or int): end of
            time period to retrieve data for
        return_comments (bool): if True, return a dictionary. if False, return
            a pandas DataFrame.

    Returns:
        pandas DataFrame, if return_comments is False. Otherwise, return a
        dictionary {"df": pandas DataFrame, "comments": comments from SILO
        call}.

    """
    if start is None:
        start = "18890101"
    else:
        try:
            start = start.strftime("%Y%m%d")
        except:
            pass
        try:
            start = start.datetime.strftime("%Y%m%d")
        except:
            pass
    if finish is None:
        finish = datetime.now().strftime("%Y%m%d")
    else:
        try:
            finish = finish.strftime("%Y%m%d")
        except:
            pass
        try:
            finish = finish.datetime.strftime("%Y%m%d")
        except:
            pass

    url = (
        f"https://www.longpaddock.qld.gov.au/cgi-bin/silo"
        f"/PatchedPointDataset.php?start={start}&finish={finish}"
        f"&station={station_code}&format=alldata&username={email}"
    )
    r = requests.get(url)
    buffer = io.StringIO(r.text)
    df = pd.read_csv(buffer, delim_whitespace=True, comment='"').iloc[1:]
    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
    for col in ("Day", "Smx", "Smn", "Srn", "Ssl", "Svp", "Ssp", "Ses", "Sp"):
        df[col] = df[col].astype(int)
    for col in (
        "T.Max",
        "T.Min",
        "Rain",
        "Evap",
        "Radn",
        "VP",
        "RHmaxT",
        "RHminT",
        "FAO56",
        "Mlake",
        "Mpot",
        "Mact",
        "Mwet",
        "Span",
        "EvSp",
        "MSLPres",
    ):
        df[col] = df[col].astype(float)
    if return_comments:
        comments = []
        for line in r.text.splitlines():
            if line.strip().startswith('"'):
                comments.append(line)
        return {"df": df, "comments": "\n".join(comments)}
    else:
        return df

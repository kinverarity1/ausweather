import logging
import sqlite3

import pandas as pd

from . import bom


logger = logging.getLogger(__name__)
__all__ = ["Database"]


class Database:
    """Cache database of weather data.

    Args:
        filename (str): path to SQLite3 database. Will be created
            if it doesn't exist.

    Attributes:
        conn (sqlite3.Connection)
        filename (str)

    """

    def __init__(self, filename="ausweather.sqlite"):
        self.conn = sqlite3.connect(filename)
        self.filename = filename

    def fetch_bom_station_lists(self, ncc_obs_codes="auto"):
        """Fetch (if necessary) and return BoM station codes for ncc obs. codes.

        Args:
            ncc_obs_codes (sequence of ints or 'auto'): obs codes to return
                stations for

        Returns:
            pandas DataFrame.

        """
        if ncc_obs_codes == "auto":
            try:
                existing_oc = pd.read_sql(
                    "select distinct ncc_obs_code from bom_stations", self.conn
                ).ncc_obs_code.values
            except:
                existing_oc = ()
            ncc_obs_codes = [
                oc for oc in bom.ncc_obs_codes.keys() if not oc in existing_oc
            ]
            logger.info(f'ncc_obs_codes="auto" -> still missing: {ncc_obs_codes}')
        dfs = [bom.fetch_bom_station_list(oc) for oc in ncc_obs_codes]
        if len(dfs):
            df = pd.concat(dfs)
            try:
                existing_df = pd.read_sql("select * from bom_stations", self.conn)
            except:
                repl_df = df
            else:
                repl_df = pd.concat([existing_df, df]).drop_duplicates()
            repl_df.to_sql("bom_stations", self.conn, if_exists="replace", index=False)
            return repl_df
        else:
            return pd.read_sql("select * from bom_stations", self.conn)

    def close(self):
        """Close SQLite3 database connection."""
        return self.conn.close()

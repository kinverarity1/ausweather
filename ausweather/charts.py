import colorsys
from datetime import date, datetime, timedelta
import io
import re

import matplotlib.pyplot as plt
from matplotlib import lines as mlines
from matplotlib import gridspec
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
import pandas as pd
import numpy as np

__all__ = ["plot_silo_station"]


def plot_silo_station(rf_annual, rf_mean, rf_annual_srn, title=""):
    colour = {"rainfall": (0 / 255, 176 / 255, 240 / 255, 1), "mean": "#bebebe"}
    rf_srn_colours = {
        "Interpolated": "tan",
        "Deaccumulated": "mistyrose",
        #     "Observations": "snow"
    }

    fig = plt.figure(figsize=(7.2, 3))
    gs = gridspec.GridSpec(2, 1, height_ratios=(4, 1))
    ax_rf = fig.add_subplot(gs[0])
    ax_interp = fig.add_subplot(gs[1], sharex=ax_rf)

    ax_rf.bar(
        rf_annual.index, rf_annual, color=colour["rainfall"], label="Annual rainfall"
    )
    ax_rf.plot(
        [rf_annual.index[0] - 0.5, rf_annual.index[-1] + 0.5],
        [rf_mean, rf_mean],
        lw=0.8,
        ls=":",
        color="darkblue",
        label="Mean annual rainfall",
    )

    bottom = np.zeros(len(rf_annual_srn))
    srn_cols = ["Date"] + list(rf_srn_colours.keys())
    srn_cols = [c for c in srn_cols if c in rf_annual_srn.columns]
    for column in srn_cols:
        rf_srn = rf_annual_srn[column]
        heights = rf_srn.values
        ax_interp.bar(
            rf_srn.index.values,
            heights,
            bottom=bottom,
            label=column,
            color=rf_srn_colours.get(column, "gray"),
        )
        bottom += heights

    for ax in (ax_rf, ax_interp):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    ax_rf.set_ylabel("Annual rainfall (mm)", fontsize="medium")
    ax_interp.set_ylabel("% of year", fontsize="medium")
    _ = plt.setp(ax_rf.get_xticklabels(), visible=False, fontsize="medium")
    _ = plt.setp(ax_interp.get_xticklabels(), fontsize="medium")
    ax_rf.set_title(title, fontsize="medium")
    ax_rf.legend(loc="best", frameon=False, fontsize="x-small", ncol=2)
    interp_leg = ax_interp.legend(
        loc="best", frameon=True, fontsize="x-small", framealpha=0.8, ncol=2
    )
    interp_leg.get_frame().set_linewidth(0)
    ax_interp.set_ylim(0, 100)
    ax_interp.xaxis.set_major_locator(mticker.MultipleLocator(5))
    fig.tight_layout()
    plt.setp(ax_interp.get_xticklabels(), rotation=90, ha="center")
    return {"fig": fig, "ax_rf": ax_rf, "ax_interp": ax_interp}

import os.path
import time
from typing import Dict, List

import numpy as np
from matplotlib import pyplot as plt

from .datalib_spec import Stats
from .utils import p_out

colors_schemeDark2 = [
    "#1b9e77",  # Teal
    "#d95f02",  # Orange
    "#7570b3",  # Purple
    "#e7298a",  # Pink
    "#66a61e",  # Green
    "#e6ab02",  # Mustard
    "#a6761d",  # Brown
    "#666666",  # Gray
]

colors = {
    "xarray": colors_schemeDark2[0],
    "polars": colors_schemeDark2[1],
    "arrow": colors_schemeDark2[2],
    "duckdb": colors_schemeDark2[3],
    "polars_parquet": colors_schemeDark2[4],
}


def plot_timing_lists(
    timings_list_per_lib: Dict[str, List[Stats]], title_time, title_mem, title_size
):
    if not os.path.exists(p_out / "figures"):
        os.mkdir(p_out / "figures")

    kinds = set(timings_list_per_lib)

    formats = list(kinds)
    times = {
        fmt: [item.time_seconds for item in timings_list_per_lib[fmt]]
        for fmt in formats
    }
    memories = {
        fmt: [item.memory_usage_MiB for item in timings_list_per_lib[fmt]]
        for fmt in formats
    }
    filesizes = {
        fmt: [item.filesize / (2**20) for item in timings_list_per_lib[fmt]]
        for fmt in formats
    }

    formats = sorted(formats, key=lambda fmt: np.mean(times[fmt]))

    # Create subplots
    fig, axs = plt.subplots(3, 1, figsize=(12, 12))

    def fill_subplot(plot_index, data, title, xlabel, ylabel):
        for i, fmt in enumerate(formats):
            x_values = np.random.default_rng().normal(
                loc=i, scale=0.1, size=len(times[fmt])
            )
            axs[plot_index].scatter(
                x_values, data[fmt], label=fmt, alpha=0.7, s=25, color=colors[fmt]
            )

        axs[plot_index].set_xticks(range(len(formats)))
        axs[plot_index].set_xticklabels(formats)
        axs[plot_index].set_xlabel(xlabel)
        axs[plot_index].set_ylabel(ylabel)
        axs[plot_index].set_title(title)
        axs[plot_index].legend()
        axs[plot_index].set_ylim(bottom=0)
        axs[plot_index].grid(True)

    fill_subplot(0, times, title_time, "Data Format", "Time (seconds)")
    fill_subplot(1, memories, title_mem, "Data Format", "Memory Usage (MiB)")
    fill_subplot(2, filesizes, title_size, "Data Format", "File size (MiB)")

    # Adjust layout
    plt.tight_layout()
    plt.savefig(p_out / "figures" / f"{str(time.time())}.png", bbox_inches="tight")


def plot_timings(timings_per_lib: Dict[str, Stats], title_time, title_mem):
    if not os.path.exists(p_out / "figures"):
        os.mkdir(p_out / "figures")

    def add_annotations(ax, bars, suffix):
        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f"{height:.2f}{suffix}",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha="center",
                va="bottom",
            )

    kinds = set(timings_per_lib)

    formats = list(kinds)

    # Create subplots
    fig, axs = plt.subplots(2, 1, figsize=(12, 12))

    xs = formats
    ys_time = [timings_per_lib[f].time_seconds for f in xs]
    ys_mem = [timings_per_lib[f].memory_usage_MiB for f in xs]

    def create_bars(i, ys, annotation_suffix, ylabel, title):
        barsi = axs[i].bar(xs, ys, width=0.4, color=[colors[x] for x in xs])
        axs[i].set_xticks(range(len(formats)))
        axs[i].set_xticklabels(formats)
        axs[i].set_xlabel("Data Format")
        axs[i].set_ylabel(ylabel)
        axs[i].set_title(title)
        axs[i].legend()
        axs[i].set_ylim(bottom=0)
        axs[i].grid(False)
        add_annotations(axs[i], barsi, annotation_suffix)

    create_bars(0, ys_time, "s", "Time (seconds)", title_time)
    create_bars(1, ys_mem, "MiB", "Memory (MiB)", title_mem)

    # Adjust layout
    plt.tight_layout()
    plt.savefig(p_out / "figures" / f"{str(time.time())}.png", bbox_inches="tight")

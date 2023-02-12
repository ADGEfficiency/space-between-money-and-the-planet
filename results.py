import pathlib

import energypylinear as epl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from numpy import inf
from rich import print
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    String,
    create_engine,
    func,
)
from sqlalchemy.orm import sessionmaker

from database import Data, base, engine, simulation_already_run


def plot_monthly_benefit(output):
    plt.style.use("bmh")
    f, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 8), sharex=True, sharey="row")

    axes[0][0].set_title("Price optimization")
    output.plot(
        ax=axes[0][0],
        y="price-profit",
        kind="bar",
        x="year",
        label="opt for price",
        color="red",
    )
    output.plot(
        ax=axes[0][1],
        y="carbon-profit",
        kind="bar",
        x="year",
        label="opt for carbon",
    )

    axes[0][1].set_title("Carbon optimization")
    output.plot(
        ax=axes[1][0],
        y="price-emissions-benefit",
        kind="bar",
        x="year",
        label="opt for price",
        color="red",
    )
    output.plot(
        ax=axes[1][1],
        y="carbon-emissions-benefit",
        kind="bar",
        x="year",
        label="opt for carbon",
    )

    axes[0][0].set_ylabel("Price Benefit $/month")
    axes[1][0].set_ylabel("Emissions Benefit tC/month")

    for ax in axes.flatten():
        ax.tick_params(axis="y", labelsize=12)
        ax.tick_params(axis="x", labelsize=10)
        ax.get_legend().remove()

    n = 12
    for ax in [axes[1][1], axes[1][0]]:
        _ = [
            l.set_visible(False)
            for (i, l) in enumerate(ax.xaxis.get_ticklabels())
            if i % n != 0
        ]
        _ = ax.tick_params(axis="x", rotation=30)

    plt.tight_layout()
    f.savefig(
        "/Users/adam/adgefficiency.github.io/assets/space-between-2023/monthly-benefit.png"
    )

    home = pathlib.Path("./tables")
    home.mkdir(exist_ok=True, parents=True)

    price_summary = pd.concat(
        [output["price-profit"], output["price-emissions-benefit"]], axis=1
    )
    price_summary = price_summary.rename(
        {"price-profit": "profit", "price-emissions-benefit": "emissions-benefit"},
        axis=1,
    )
    price_summary["objective"] = "price"
    carbon_summary = pd.concat(
        [output["carbon-profit"], output["carbon-emissions-benefit"]], axis=1
    )
    carbon_summary = carbon_summary.rename(
        {"carbon-profit": "profit", "carbon-emissions-benefit": "emissions-benefit"},
        axis=1,
    )
    carbon_summary["objective"] = "carbon"

    summary = pd.concat([price_summary, carbon_summary], axis=0)

    grp = summary.groupby("objective").agg(
        negative_profit=("profit", lambda x: 100 * (x < 0).sum() / len(x)),
        negative_emissions_benefit=(
            "emissions-benefit",
            lambda x: 100 * (x < 0).sum() / len(x),
        ),
        months=("profit", lambda x: len(x)),
    )
    (home / "monthly-benefit.txt").write_text(grp.to_markdown())


def plot_monthly(output):
    plt.style.use("bmh")
    f, axes = plt.subplots(nrows=3, figsize=(15, 8), sharex=True)

    axes[0].set_title("Price Delta $k/month")
    output.plot(ax=axes[0], y="variance-emissions", kind="bar", x="year", color="red")
    axes[0].set_ylabel("$k/month")
    axes[1].set_title("Carbon Delta tC/month")
    axes[0].set_ylabel("tC/month")
    output.plot(ax=axes[1], y="variance-profit", kind="bar", x="year")
    axes[2].set_title("Carbon Price $/tC")
    axes[0].set_ylabel("$/tC")
    output.plot(ax=axes[2], y="carbon-price", kind="bar", x="year", color="#467821")
    plt.tight_layout()

    for ax in axes.flatten():
        ax.tick_params(axis="y", labelsize=12)
        ax.tick_params(axis="x", labelsize=10)

    n = 12
    for ax in [axes[2]]:
        _ = [
            l.set_visible(False)
            for (i, l) in enumerate(ax.xaxis.get_ticklabels())
            if i % n != 0
        ]
        _ = ax.tick_params(axis="x", rotation=30)
        ax.set_xlabel("")

    f.savefig(
        "/Users/adam/adgefficiency.github.io/assets/space-between-2023/monthly.png"
    )


def plot_annual(annual):

    f, axes = plt.subplots(nrows=3, figsize=(15, 8), sharex=True)

    axes[0].set_title("Price Delta $k/year")
    for i, v in enumerate(annual["variance-profit"]):
        axes[0].text(i - 0.07, v - 100, f"{v:.0f}", fontweight="bold", color="white")
    annual.plot(ax=axes[0], y="variance-profit", kind="bar", color="red")

    axes[1].set_title("Carbon Delta tC/year")
    for i, v in enumerate(annual["variance-emissions"]):
        axes[1].text(i - 0.1, v - 800, f"{v:.0f}", fontweight="bold", color="white")

    annual.plot(ax=axes[1], y="variance-emissions", kind="bar")

    axes[2].set_title("Carbon Price $/tC")
    annual.plot(ax=axes[2], y="carbon-price", kind="bar", color="#467821")
    for i, v in enumerate(annual["carbon-price"]):
        axes[2].text(i - 0.08, v - 20, f"{v:.1f}", fontweight="bold", color="white")

    axes[2].set_xlabel("")
    plt.tight_layout()

    for ax in axes:
        ax.tick_params(axis="x", labelsize=12)
        ax.tick_params(axis="y", labelsize=12)
        ax.get_legend().remove()

        _ = ax.tick_params(axis="x", rotation=0)

    f.savefig(
        "/Users/adam/adgefficiency.github.io/assets/space-between-2023/annual.png"
    )


if __name__ == "__main__":
    print("starting results")
    """
    select rows from simulation table
    sort by date and objective
    then iterate over in 2's
    """
    Session = sessionmaker(bind=engine)
    with Session() as session:
        results = session.query(Data).order_by(Data.date, Data.objective).all()

    print(len(results))
    output = []
    for left, right in zip(results[::2], results[1::2]):
        print(left.feasible, right.feasible, left.objective, right.objective)
        assert left.objective != right.objective

        assert left.feasible
        # if left.feasible == right.feasible:
        assert left.year == right.year
        assert left.month == right.month
        assert left.objective != right.month
        out = {
            "year": left.year,
            "month": left.month,
            f"{left.objective}-simulation": f"{left.simulation_id}",
            f"{right.objective}-simulation": f"{right.simulation_id}",
        }
        print(out)
        import pathlib

        for dataset in ["interval-data", "simulation"]:
            for objective in ["price", "carbon"]:
                out[f"{objective}-{dataset}"] = pd.read_parquet(
                    pathlib.Path("./data/results")
                    / out[f"{objective}-simulation"]
                    / f"{dataset}.parquet"
                )

        #  now calculate accounts & dif -> results table
        import energypylinear as epl

        price_id = epl.IntervalData(
            electricity_prices=out["price-interval-data"]["electricity_prices"],
            electricity_carbon_intensities=out["price-interval-data"][
                "electricity_carbon_intensities"
            ],
        )
        np.seterr(all="raise")

        def validate(interval_data):
            assert np.isnan(interval_data.electricity_prices).sum() == 0

        validate(price_id)
        price = epl.get_accounts(price_id, out["price-simulation"])

        carbon_id = epl.IntervalData(
            electricity_prices=out["carbon-interval-data"]["electricity_prices"],
            electricity_carbon_intensities=out["carbon-interval-data"][
                "electricity_carbon_intensities"
            ],
        )
        validate(carbon_id)
        carbon = epl.get_accounts(carbon_id, out["carbon-simulation"])

        variance = price - carbon
        out["cost"] = variance.cost
        out["emissions"] = variance.emissions

        result = {
            #  units are wrong!
            "carbon-cost": carbon.cost,
            "carbon-profit": carbon.profit,
            "carbon-emissions": carbon.emissions,
            "carbon-emissions-benefit": -1 * carbon.emissions,
            "price-cost": price.cost,
            "price-profit": price.profit,
            "price-emissions": price.emissions,
            "price-emissions-benefit": -1 * price.emissions,
            "variance-cost": variance.cost,
            "variance-emissions": variance.emissions,
            #  TODO want variance.profit in epl
            "variance-profit": -1 * variance.cost,
            "carbon-price": -1 * variance.cost / variance.emissions,
            "year": left.year,
            "month": left.month,
            "day": left.day,
            "date": left.date,
        }
        print(result)
        print("")
        output.append(result)

    output = pd.DataFrame(output)

    output.index = pd.to_datetime(output["date"])
    annual = output.groupby(output.index.year).agg(
        {"variance-profit": "sum", "variance-emissions": "sum"}
    )
    annual["carbon-price"] = annual["variance-profit"] / annual["variance-emissions"]
    annual["variance-profit"] = annual["variance-profit"] / 1000

    plot_monthly_benefit(output)
    plot_monthly(output)
    plot_annual(annual)
    output.to_parquet("./data/results/monthly.parquet")
    annual.to_parquet("./data/results/annual.parquet")

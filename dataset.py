import pathlib

import nemdata
import numpy as np
import pandas as pd
from numpy import inf
from rich import print

base = pathlib.Path("./data")
base.mkdir(exist_ok=True, parents=True)


def load_nem_data(table, region, start, end, region_col="REGIONID", columns=None):
    data = nemdata.download(start, end, table)
    data = nemdata.load(table, columns=columns)[table]

    mask = data[region_col] == region
    data = data.loc[mask, :]
    data = data.set_index("interval-start")
    return data.loc[start:end, :]


def process_prices(prices, start, end):
    #  hmmmm - happened when i introduced 2014 trading prices
    prices = prices.drop_duplicates()
    #  TODO move into nem-data
    prices = prices.reset_index()
    print("sort")
    prices = prices.sort_values(["interval-start", "INTERVENTION"])
    print("dupes")
    prices = prices.drop_duplicates(subset=["interval-start"], keep="first")
    #  TODO
    # assert (prices["INTERVENTION"] == 1).sum() == 0
    prices = prices.set_index("interval-start")
    print("resample")
    prices = prices.loc[:, "RRP"].to_frame()
    prices = prices.rename({"RRP": "electricity_price_$_mwh"}, axis=1)
    prices = prices.resample("5T").ffill()
    return prices.loc[start:end, :]


def process_marginal_carbon_intensities(
    marginal_generator: pd.DataFrame, sites: pd.DataFrame, start, end
):
    marginal_generator = marginal_generator.loc[start:end, :]

    mask = marginal_generator["Market"] == "Energy"
    marginal_generator = marginal_generator.loc[mask, :]

    mask = marginal_generator["DispatchedMarket"] == "ENOF"
    marginal_generator = marginal_generator.loc[mask, :]

    marginal_generator = marginal_generator.reset_index()
    marginal_generator = marginal_generator.merge(
        sites[["DUID", "REGIONID", "CO2E_EMISSIONS_FACTOR"]],
        left_on="Unit",
        right_on="DUID",
        how="left",
    )
    mask = marginal_generator["CO2E_EMISSIONS_FACTOR"].isnull()
    marginal_generator = marginal_generator.loc[~mask, :]
    assert marginal_generator.isnull().sum().sum() == 0
    marginal_generator["carbon_generation_tc_h"] = (
        marginal_generator["CO2E_EMISSIONS_FACTOR"] * marginal_generator["Increase"]
    )

    # Now group all the marginal generators across the intervals:

    marginal_generator = marginal_generator.set_index("interval-start")
    intensity = marginal_generator.groupby(marginal_generator.index).agg(
        {
            "carbon_generation_tc_h": "sum",
            "Increase": "sum",
        }
    )
    intensity = intensity.rename({"Increase": "electricity_mw"}, axis=1)
    intensity["marginal_carbon_intensity_tc_mwh"] = (
        intensity["carbon_generation_tc_h"] / intensity["electricity_mw"]
    )

    intensity["marginal_carbon_intensity_tc_mwh"] = np.nan_to_num(
        intensity["marginal_carbon_intensity_tc_mwh"].values, neginf=0
    )
    return intensity


def pipeline(start, end):
    region = "SA1"
    prices = load_nem_data(
        "dispatch-price",
        region,
        start,
        end,
        columns=["interval-start", "RRP", "INTERVENTION", "REGIONID"],
    )
    prices = process_prices(prices, start, end)

    sites = pd.read_csv(
        "http://www.nemweb.com.au/Reports/CURRENT/CDEII/CO2EII_AVAILABLE_GENERATORS.CSV",
        skiprows=1,
    ).iloc[:-1, :]
    sites.head(2)

    marginal_generator = load_nem_data(
        "nemde",
        region,
        start,
        end,
        region_col="RegionID",
        columns=[
            "interval-start",
            "Market",
            "DispatchedMarket",
            "Unit",
            "Increase",
            "RegionID",
        ],
    )
    intensity = process_marginal_carbon_intensities(
        marginal_generator, sites, start, end
    )
    ds = prices.merge(
        intensity, how="left", left_index=True, right_index=True
    ).sort_index()
    ds = ds.loc[
        :,
        [
            "electricity_price_$_mwh",
            "marginal_carbon_intensity_tc_mwh",
        ],
    ]
    carbon_intensity = ds["marginal_carbon_intensity_tc_mwh"]
    ds["marginal_carbon_intensity_tc_mwh"] = carbon_intensity.clip(
        lower=-100, upper=100
    )
    ds = ds.dropna(axis=0)
    ds = ds.sort_index()
    print(" write: dataset.parquet")
    ds.to_parquet(base / "dataset.parquet")
    return ds


if __name__ == "__main__":
    start = "2014-01-01"
    end = "2022-12-31"
    ds = pipeline(start, end)

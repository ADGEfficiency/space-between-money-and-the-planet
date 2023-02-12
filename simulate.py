import concurrent
import datetime
import pathlib
import time
import uuid

import energypylinear as epl
import pandas as pd
from numpy import inf
from rich import print

from database import (
    base,
    simulation_already_run,
    write_simulation_results_to_disk,
    write_simulation_results_to_sqlite,
)


def year_month_to_date(year_month: tuple) -> str:
    return f"{year_month[0]}-{str(year_month[1]).zfill(2)}-01"


def optimize(subset, objective, mdl):
    tic = time.perf_counter()
    simulation = mdl.optimize(
        electricity_prices=subset["electricity_price_$_mwh"],
        electricity_carbon_intensities=subset["marginal_carbon_intensity_tc_mwh"],
        objective=objective,
        allow_infeasible=False,
        #  TODO
        # allow_spill=False,
        freq_mins=5,
    )
    toc = time.perf_counter() - tic
    return simulation, toc


def simulate(data):
    year_month, subset, mdl, objective = data
    print(f"starting:\n {year_month=} {objective=}\n")

    simulation_id = str(uuid.uuid4())

    meta = {
        "start": datetime.datetime.fromisoformat(subset.index[0].isoformat()),
        "date": str(year_month_to_date(year_month)),
        "year": int(year_month[0]),
        "month": int(year_month[1]),
        "day": 1,
        "objective": objective,
        "simulation_id": simulation_id,
    }

    simulation, run_time = optimize(subset, objective, mdl)
    meta["feasible"] = simulation.feasible
    meta["spill"] = simulation.spill
    meta["run_time"] = run_time
    print(f"finished:\n {meta}\n")

    write_simulation_results_to_disk(simulation_id, meta, subset, simulation)
    write_simulation_results_to_sqlite(simulation_id, meta)


if __name__ == "__main__":
    ds = pd.read_parquet(base / "dataset.parquet")
    mdl = epl.Battery(power_mw=2, capacity_mwh=4, efficiency=1.0)

    simulation_data = []
    for year_month, subset in ds.groupby([ds.index.year, ds.index.month]):
        for objective in ["price", "carbon"]:

            if not simulation_already_run(year_month_to_date(year_month), objective):
                simulation_data.append((year_month, subset, mdl, objective))

            else:
                print(f"{year_month} {objective} already simulated")

    print("simulations:")
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(simulate, simulation_data))

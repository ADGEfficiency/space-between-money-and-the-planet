import matplotlib.pyplot as plt

plt.style.use("bmh")

f, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 8), sharex=True)

import pandas as pd
from sqlalchemy.orm import sessionmaker

from database import Data, base, engine, simulation_already_run

Session = sessionmaker(bind=engine)
with Session() as session:
    results = session.query(Data).order_by(Data.date, Data.objective).all()

carbon = results[0]
price = results[1]

import pathlib

price = pd.read_parquet(
    pathlib.Path("./data/results") / price.simulation_id / "simulation.parquet"
)
carbon = pd.read_parquet(
    pathlib.Path("./data/results") / carbon.simulation_id / "simulation.parquet"
)
m = 24 * 12 * 6
n = 24 * 12 * 10

pd.DataFrame(price).iloc[m:n,].plot(
    y="electricity_prices",
    ax=axes[0][0],
    color="red",
    label="Price [$/MWh]",
)
pd.DataFrame(price).iloc[m:n,].plot(
    y="battery-final_charge_mwh",
    ax=axes[1][0],
    color="red",
    label="Final Charge [MWh]",
)
pd.DataFrame(carbon).iloc[m:n,].plot(
    y="battery-final_charge_mwh",
    ax=axes[1][1],
    label="Final Charge [MWh]",
)
pd.DataFrame(carbon).iloc[m:n,].plot(
    y="electricity_carbon_intensities",
    ax=axes[0][1],
    label="Carbon Intensity [tC/MWh]",
)

axes[0][0].set_title("Optimize for Price")
axes[0][1].set_title("Optimize for Carbon")

axes[0][0].set_ylabel('$/MWh')
axes[0][1].set_ylabel('tC/MWh')
axes[1][0].set_ylabel('MWh')
axes[1][1].set_ylabel('MWh')

for ax in axes.flatten():
    ax.legend(loc='upper left')

plt.tight_layout()
f.savefig("/Users/adam/adgefficiency.github.io/assets/space-between-2023/panel.png")
# for ax in axes.flatten():
#    ax.get_legend().remove()

import datetime
import json
import pathlib

from numpy import inf
from rich import print
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
base = pathlib.Path("./data")
base.mkdir(exist_ok=True, parents=True)

engine = create_engine("sqlite:///data/database.sqlite")


class Data(Base):
    __tablename__ = "data"

    id = Column(Integer, primary_key=True)
    start = Column(DateTime)
    date = Column(String)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    objective = Column(String(20))
    feasible = Column(Boolean)
    spill = Column(Boolean)
    run_time = Column(Float)
    simulation_id = Column(String(36), unique=True)


Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)


def simulation_id_to_folder(simulation_id: str, create: bool = True) -> pathlib.Path:
    path = base / "results" / str(simulation_id)

    if create:
        path.mkdir(exist_ok=True, parents=True)
    return path


def simulation_already_run(date: str, objective: str) -> bool:
    try:
        with Session() as session:
            result = (
                session.query(Data)
                .filter(Data.date == date, Data.objective == objective)
                .first()
            )
            return bool(result)
    #  catch when database no exist yet
    except:
        return False


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return super(JSONEncoder, self).default(obj)


def write_simulation_results_to_disk(simulation_id, meta, subset, simulation):
    path = simulation_id_to_folder(simulation_id)
    (path / "meta.json").write_text(json.dumps(meta, cls=JSONEncoder))

    subset.to_parquet((path / "input-interval-data.parquet"))
    simulation.simulation.to_parquet((path / "simulation.parquet"))
    simulation.interval_data.to_dataframe().to_parquet((path / "interval-data.parquet"))


def write_simulation_results_to_sqlite(simulation_id, meta):
    with Session() as session:
        session.add(Data(**meta))
        session.commit()


def process_simulation():
    Session = sessionmaker(bind=engine)

    with Session() as session:
        results = session.query(Data).order_by(Data.date, Data.objective).all()
    return results

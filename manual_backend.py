import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from cachetools import cached, TTLCache

from hdensity.dto.payload import Payload
from hdensity.util.util_func import *
from server import Main

loadenv()
client = get_mqtt_client()
main = Main()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

location_detail_mapper = dict(pd.read_sql("SELECT * FROM location_detail", main.db_engine)[["location_id", "location_name"]].values.tolist())

@app.get("/")
@cached(cache=TTLCache(maxsize=1, ttl=10))
def index():
    return pd.read_sql(f"""WITH ldtl AS (
        SELECT ld.*, tl.datetime, tl.total FROM location_detail as ld LEFT JOIN total_log as tl ON ld.location_id = tl.location_id 
        )
        SELECT location_name, datetime as "time", total FROM( SELECT *, ROW_NUMBER() OVER(PARTITION BY location_id ORDER BY datetime DESC) AS rn
        FROM ldtl) AS a
        ORDER BY time DESC LIMIT {len(set(main.sensor_location_mapper.values()))}
    """, main.db_engine).to_dict(orient="records")

@app.get("/in/{sensor_id}")
def check_in(
    sensor_id: int,
    ):
    sensor_id = str(sensor_id)
    if sensor_id not in main.sensor_location_mapper.keys():
        return f"Invalid sensor ID"
    print(main.sensor_location_mapper)
    client.publish(os.environ.get("ACTION_TOPIC"), Payload(sensor_id, 1).serialize())
    return f"SENSOR_ID: {sensor_id} at location: {location_detail_mapper[main.sensor_location_mapper[sensor_id]]} increment by 1."

@app.get("/out/{sensor_id}")
def check_out(
    sensor_id: int,
    ):
    sensor_id = str(sensor_id)
    if sensor_id not in main.sensor_location_mapper.keys():
        return f"Invalid sensor ID"
    client.publish(os.environ.get("ACTION_TOPIC"), Payload(sensor_id, 0).serialize())
    return f"SENSOR_ID: {sensor_id} at location: {location_detail_mapper[main.sensor_location_mapper[sensor_id]]} decrement by 1."



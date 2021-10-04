import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


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

@app.get("/")
def index():
    query = f"""WITH ldtl AS (
        SELECT ld.*, tl.datetime, tl.total FROM location_detail as ld LEFT JOIN total_log as tl ON ld.location_id = tl.location_id 
        )
        SELECT location_name, datetime as "time", total FROM( SELECT *, ROW_NUMBER() OVER(PARTITION BY location_id ORDER BY datetime DESC) AS rn
        FROM ldtl) AS a
        ORDER BY time DESC LIMIT {len(set(main.sensor_location_mapper.values()))}
    """
    return pd.read_sql(query, main.db_engine).to_dict(orient="records")

@app.get("/in/{sensor_id}")
def check_in(
    sensor_id: int,
    ):
    if str(sensor_id) not in main.sensor_location_mapper:
        return f"Invalid sensor ID"
    client.publish(os.environ.get("ACTION_TOPIC"), Payload(sensor_id, 1).serialize())
    return f"SENSOR_ID: {sensor_id} at location: f{main.sensor_location_mapper[sensor_id]} increment by 1."

@app.get("/out/{sensor_id}")
def check_out(
    sensor_id: int,
    ):
    if str(sensor_id) not in main.sensor_location_mapper:
        return f"Invalid sensor ID"
    client.publish(os.environ.get("ACTION_TOPIC"), Payload(sensor_id, 0).serialize())
    return f"SENSOR_ID: {sensor_id} at location: f{main.sensor_location_mapper[sensor_id]} decrement by 1."



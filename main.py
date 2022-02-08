from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import List
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.eventhub.exceptions import EventHubError

CONNECTION_STR = 'Endpoint=sb://evhns-das-dev-westeu-001.servicebus.windows.net/;SharedAccessKeyName=das;SharedAccessKey=2jy78rLc8wLVCgKBT/5YiGjaEckkClQfaOUleTT7xhg='
EVENTHUB_NAME = 'evh-das-dev-westeu-001'

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)

# pydantic models
class Satellite(BaseModel):
    satellite: str
    txt1: str
    txt2: str
    txt3: str
    num1: int
    num2: int
    num3: int
    ts_unix: int
    timestamp: str

app = FastAPI()

satellite_list = ['FM09', 'FM05', 'FM07', 'FM10', 'FM12']
satdata_map = {}

for satellite in satellite_list:
    satdata_map[satellite] = []


# routes
@app.get("/ping")
def pong():
    return {"ping": "pong!"}


@app.get("/satellites", response_model=List[str])
def get_satellites():
    """Get all satellites in list form."""
    return satellite_list


@app.get("/satdata/{satellite}", response_model=List[Satellite])
def get_satdata(satellite: str):
    """Get all data for a specified satellite."""
    if satellite in satellite_list:
        return satdata_map.get(satellite)
    else:
        raise HTTPException(status_code=404, detail="Satellite not found")


# @app.get("/records/{satellite}")
# def get_records(satellite: str):
#     """Get a count of records for a specified satellite."""
#     if satellite in satellite_list:
#         return satdata_map[satellite].keys()
#     else:
#         raise HTTPException(status_code=404, detail="Satellite not found")


@app.post("/post_satdata", status_code=status.HTTP_201_CREATED)
async def post_satdata(satdata: Satellite):
    """Post new sat data stream for satellite"""
    satellite = satdata.satellite
    if satellite in satellite_list:
        msgdata = str(satdata)
        # Without specifying partition_id or partition_key
        # the events will be distributed to available partitions via round-robin.
        event_data_batch = await producer.create_batch()
        event_data_batch.add(EventData(msgdata))
        await producer.send_batch(event_data_batch)

        return "Satellite data posted"
    else:
        raise HTTPException(status_code=404, detail="Satellite not found")

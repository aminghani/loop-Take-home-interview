from fastapi import FastAPI, BackgroundTasks
import util
import os
import data.query as query
import pandas as pd
from fastapi.responses import StreamingResponse
import io


CACHE_DIR_RESULT = "./cache/result.csv"
CACHE_DIR_DATA_STATUS = "./cache/store status.csv"
CACHE_DIR_DATA_HOURS = "./cache/Menu hours.csv"
CACHE_DIR_DATA_TIMEZONES = "./cache/store timezones.csv"


app = FastAPI()


def trigger_process():
    res = util.compute_week_day_for_all(util.store_status)
    util.to_csv(res, CACHE_DIR_RESULT)


@app.get("/trigger_report")
async def trigger(background_tasks: BackgroundTasks):
    """
    first looks at cache folder and if data is not cached, reads from database and cache data,
    then begin computation.
    :param background_tasks: fastapi BackgroundTasks
    """
    if (
        not os.path.exists(CACHE_DIR_DATA_STATUS)
        or not os.path.exists(CACHE_DIR_DATA_HOURS)
        or not os.path.exists(CACHE_DIR_DATA_TIMEZONES)
    ):
        store_status = query.read_sql_data("store_status")
        menu_hours = query.read_sql_data("menu_hours")
        store_timezones = query.read_sql_data("store_timezones")
        store_status.to_csv(CACHE_DIR_DATA_STATUS)
        menu_hours.to_csv(CACHE_DIR_DATA_HOURS)
        store_timezones.to_csv(CACHE_DIR_DATA_TIMEZONES)

    if os.path.exists(CACHE_DIR_RESULT):
        os.remove(CACHE_DIR_RESULT)
    background_tasks.add_task(trigger_process)
    return {"message": "Task submitted in the background"}


@app.get("/get_report/{store_id}")
async def read_item(store_id):
    """
    giving a store id in url, return uptimes and downtimes according to that store in a csv file,
    if computation is not complete, returns task is running.
    :param store_id: id of the store
    :return: csv file or json response.
    """
    if not os.path.exists(CACHE_DIR_RESULT):
        return {"status": "task is running or has not been triggered"}
    result = pd.read_csv(CACHE_DIR_RESULT)
    store_id = int(store_id)
    result_store = result[result.store_id == store_id]
    headers = {"Content-Disposition": "attachment; filename=data.csv"}
    csv_bytes = result_store.to_csv(index=False).encode("utf-8")

    csv_file = io.BytesIO(csv_bytes)
    return StreamingResponse(
        iter(csv_file.readline, b""), media_type="text/csv", headers=headers
    )

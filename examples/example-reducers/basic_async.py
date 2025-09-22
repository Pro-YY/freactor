import asyncio
import logging
from random import random

from freactor import StatusCode, async_freducer as freducer

log = logging.getLogger(__name__)

SUCCESS = StatusCode.SUCCESS
FAILURE = StatusCode.FAILURE
RETRY = StatusCode.RETRY
ABORT = StatusCode.ABORT


@freducer(3, 1)
async def s1(t_data):
    task_id = t_data.get("_task_id", "?")
    log.info(f"[task {task_id}] s1 running...")
    log.info(f"[task {task_id}] data={t_data}")
    await asyncio.sleep(1)
    r = random()
    if r < 0.9:
        return StatusCode.SUCCESS, {"s1": 1}, "s1 general success"
    else:
        return StatusCode.ABORT, {"s1": 1}, "s1 fail, abort"


@freducer()
async def s2(t_data):  # cleanup step of s1
    task_id = t_data.get("_task_id", "?")
    log.info(f"[task {task_id}] s2 running...")
    log.info(f"[task {task_id}] data={t_data}")
    await asyncio.sleep(1)
    r = random()
    if r < 0.9:
        return StatusCode.SUCCESS, {"s2": 2}, "s2 general success"
    else:
        raise Exception("Woo! s2 raised!")


@freducer(3, 0)
async def s3(t_data):
    task_id = t_data.get("_task_id", "?")
    log.info(f"[task {task_id}] s3 running...")
    log.info(f"[task {task_id}] data={t_data}")
    await asyncio.sleep(1)
    r = random()
    if r < 0.3:
        return StatusCode.SUCCESS, {"s3": 1}, "s3 general success"
    else:
        raise Exception("Woo! s3 raised!")


@freducer()
async def s4(t_data):  # cleanup step of s3
    task_id = t_data.get("_task_id", "?")
    log.info(f"[task {task_id}] s4 running...")
    log.info(f"[task {task_id}] data={t_data}")
    await asyncio.sleep(1)
    r = random()
    if r < 0.9:
        return StatusCode.SUCCESS, {"s4": 4}, "s4 general success"
    else:
        raise Exception("Woo! s4 raised!")


@freducer()
async def s5(t_data):
    task_id = t_data.get("_task_id", "?")
    log.info(f"[task {task_id}] s5 running...")
    log.info(f"[task {task_id}] data={t_data}")
    await asyncio.sleep(1)
    r = random()
    if r < 0.2:
        return StatusCode.SUCCESS, {"s5": 5}, "s5 general success"
    else:
        raise Exception("Woo! s5 raised!")


import httpx

@freducer()
async def call_http_api(t_data):
    await asyncio.sleep(random() * 10)
    async with httpx.AsyncClient(timeout=5) as client:
        log.info(f"[task {t_data.get('_task_id', '?')}] calling http api...")
        r = await client.get("https://httpbin.org/get")
        # r = await client.get("https://httpbin.org/delay/1")
        return StatusCode.SUCCESS, {"api_status": r.status_code}, "http ok"

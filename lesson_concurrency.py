import concurrent
import multiprocessing
import os
import threading
import time
from decimal import getcontext, Decimal

import aiohttp
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor

PRECISIONS = (5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000,)
BASE_URL = "https://public-src.s3.us-east-2.amazonaws.com/study/async/"
FILES_TO_FETCH = [
    "ford_escort.csv",
    "cities.csv",
    "hw_25000.csv",
    "mlb_teams_2012.csv",
    "nile.csv",
    "homes.csv",
    "hooke.csv",
    "lead_shot.csv",
    "news_decline.csv",
    "snakes_count_10000.csv",
    "trees.csv",
    "zillow.csv"
]
START_TIME = time.time()


def pi(precision):
    started = time.time()
    getcontext().prec = precision

    result = sum(1/Decimal(16)**k * (Decimal(4)/(8*k+1) - Decimal(2)/(8*k+4) - Decimal(1)/(8*k+5) - Decimal(1)/(8*k+6))
                 for k in range(precision))
    duration = time.time() - started
    print(f'{threading.current_thread()}, {os.getpid()} - {duration} seconds: {result}')
    return result


def fetch(file):
    started = time.time()
    time.sleep(1)
    response = requests.get(BASE_URL + file)
    data = response.text
    duration = time.time() - started
    print(f'{threading.current_thread()}, {os.getpid()} - {file} - {len(data)} characters - {duration} seconds; ')
    return data


async def fetch_async(file):
    started = time.time()
    await asyncio.sleep(1)
    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL + file) as response:
            data = await response.text()
            duration = time.time() - started
            print(f'{file} - {len(data)} characters - {duration} seconds')
            return data


def run_pi_one_by_one():
    for precision in PRECISIONS:
        data = pi(precision)
        # print(data)


def run_fetch_one_by_one():
    for file in FILES_TO_FETCH:
        fetch(file)


def run_pi_in_processes():
    _run_in_parallel(multiprocessing.Process, PRECISIONS, pi)
    # with multiprocessing.Pool(len(PRECISIONS)) as pool:
    #     pool.map(pi, PRECISIONS)


def run_fetch_in_processes():
    _run_in_parallel(multiprocessing.Process, FILES_TO_FETCH, fetch)


def run_fetch_in_processes_concurrent():
    with concurrent.futures.ProcessPoolExecutor() as executor:
        _run_fetch_in_executor(executor, FILES_TO_FETCH, fetch)


def run_pi_in_threads():
    _run_in_parallel(threading.Thread, PRECISIONS, pi)


def run_fetch_in_threads():
    _run_in_parallel(threading.Thread, FILES_TO_FETCH, fetch)


def run_fetch_in_threads_concurrent():
    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        return _run_fetch_in_executor(executor, FILES_TO_FETCH, fetch)


async def run_pi_async():
    tasks = [pi(precision) for precision in PRECISIONS]
    await asyncio.gather(*tasks)


async def run_fetch_async():
    tasks = [fetch_async(file) for file in FILES_TO_FETCH]
    return await asyncio.gather(*tasks)


def _run_in_parallel(environment, tasks, fun):
    items = []
    for task in tasks:
        item = environment(target=fun, args=(task,))
        item.start()
        items.append(item)
    for item in items:
        item.join()


def _run_fetch_in_executor(executor, tasks, fun):
    futures = []
    for task in tasks:
        futures.append(executor.submit(fun, task))
    for future in concurrent.futures.as_completed(futures):
        future.result()


if __name__ == "__main__":
    """ 
     Concurrency means that two or more calculations happen within the same time frame.
     Parallelism means that two or more calculations happen at the same moment.
     Parallelism is therefore a specific case of concurrency. It requires multiple CPU units or cores.
    
     GIL - global interpreter lock is a mechanism used in Python interpreter to synchronize
     the execution of threads so that only one native thread can execute at a time,
     even if run on a multi-core processor.
     
     Python has three modules for concurrency: multiprocessing, threading, and asyncio.
     multiprocessing - good for CPU intensive tasks
     threading - good for I/O bound tasks when libraries (e.g. requests, time) cannot cooperate with asyncio
     asyncio - good for I/O bound tasks, all libraries should be compatible (awaitable) with asyncio 
     
     multiprocessing and threading are the low-level APIs engulfed by high-level concurrent.futures API.
     With asyncio we can combine all three form of concurrency: multiprocessing, threading, and asyncio itself
    """

    # 1. Traditional approach
    # run_pi_one_by_one()
    # run_fetch_one_by_one()

    # 2. Parallelism based on processes (one process per one CPU core, isolated memory,
    # personal separate GIL per one CPU core) boosts mathematical computation tasks performance
    # run_pi_in_processes()
    # run_fetch_in_processes()
    # run_fetch_in_processes_concurrent()

    # 2. Pseudo-parallelism based on threads (up to 5 threads per one CPU core, shared memory,
    # common GIL) boosts I/O tasks performance
    # run_pi_in_threads()
    # run_fetch_in_threads()
    # run_fetch_in_threads_concurrent()

    # 4. Asynchronous approach (one thread in one process - main thread, unblocking calls)
    # boosts I/O tasks performance
    # asyncio.get_event_loop().run_until_complete(run_pi_async())
    result = asyncio.get_event_loop().run_until_complete(run_fetch_async())

    total_duration = time.time() - START_TIME
    print(f"total time: {total_duration} s")

import time
from multiprocessing import Process, Queue
from queue import Empty

TIMEOUT = 12


def a(queue):
    try:
        time.sleep(5)
        queue.put({'a': ['a1']})
    except Exception:
        pass


def b(queue):
    try:
        time.sleep(10)
        queue.put({'b': ['b1', 'b2']})
    except Exception:
        pass


def c(queue):
    try:
        time.sleep(15)
        queue.put({'c': ['c1', 'c2', 'c3']})
    except Exception:
        pass


if __name__ == '__main__':
    q = Queue()
    args = (q,)

    jobs = []

    for fun in (a, b, c,):
        job = Process(target=fun, args=args, daemon=True, name=f'process-fun-{fun.__name__}')
        job.start()
        jobs.append(job)
        print(f'{job} started')

    end = time.time() + TIMEOUT
    for job in jobs:
        timeout = end - time.time()
        if timeout < 0:
            timeout = 0
        print(f'waiting for {job} to complete within {timeout} seconds')
        job.join(timeout=timeout)

        if job.is_alive():
            print(f'{job} is still running, terminating...')
            job.terminate()
        else:
            print(f'{job} completed in less than {TIMEOUT} seconds')

    results = {}
    while True:
        try:
            results.update(q.get_nowait())
        except Empty:
            break
    print(f'Results: {results}')

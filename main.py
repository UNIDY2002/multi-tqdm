from multiprocessing import Lock, Process, Queue
from queue import Empty
from sys import stderr
from time import time

from tqdm import tqdm
from tqdm.utils import _screen_shape_wrapper


def _worker(index: int, work, src: Queue, shared: Queue, lock: Lock, length: int, start: float, col: int):
    """
    A worker that continually processes data from the given source, update tqdm progress,
    until all data has been processed.
    """
    while True:
        try:
            # Do work.
            work(index, src.get(timeout=0.2))

            # Update tqdm.
            lock.acquire()
            new_id = shared.get() + 1
            stderr.write(tqdm.format_meter(new_id, length, time() - start, col) + '\r')
            shared.put(new_id)
            lock.release()

        except Empty:
            # All data has been processed.
            return


def parallel(work, data: list, process_cnt: int):
    """
    Do parallel work on a list of data and meanwhile maintains a progress bar.

    This method differs from other similar implementations in that `work` can know which
    process it is running on, by receiving the index of the process as a parameter.

    For example:
    ```
    def my_worker(index: int, x: int):
    with open('%s.txt' % index, 'a') as f:
        f.write('%s\n' % x)

    parallel(my_worker, list(range(30)), 4)
    ```
    """
    # Prepare data for parallel use.
    src = Queue()
    for item in data:
        src.put(item)
    length = len(data)

    # Record progress for tqdm.
    shared = Queue()
    shared.put(0)
    lock = Lock()
    start = time()

    # Try to get the width of the console.
    try:
        col = _screen_shape_wrapper()(stderr)[0] // 2
    except TypeError:
        col = 10
    stderr.write(tqdm.format_meter(0, length, time() - start, col) + '\r')

    processes = [Process(target=_worker, args=(i, work, src, shared, lock, length, start, col))
                 for i in range(process_cnt)]

    for process in processes:
        process.start()
    for process in processes:
        process.join()

    stderr.write('\n')


def my_worker(index: int, x: int):
    with open('%s.txt' % index, 'a') as f:
        f.write('%s\n' % x)


if __name__ == '__main__':
    parallel(my_worker, list(range(30)), 4)

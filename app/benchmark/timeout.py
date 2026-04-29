import signal
from contextlib import contextmanager


class BenchmarkTimeoutError(Exception):
    pass


@contextmanager
def timeout(seconds: int):
    def handler(signum, frame):
        raise BenchmarkTimeoutError(
            f"Execution exceeded timeout of {seconds}s"
        )

    previous_handler = signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
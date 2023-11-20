import time
from commons.logger import logger


def timeit(func: callable):
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.info(
            f'Logger Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

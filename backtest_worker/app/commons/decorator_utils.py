import sys
import time
import logging


def timeit(func: callable):
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logging.info(
            f'Logger Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper


# def capture_print(func: callable):
#     def wrapper(*args, **kwargs):
#         import io
#         # Redirect stdout to capture the print output
#         original_stdout = sys.stdout
#         sys.stdout = io.StringIO()

#         # Call the original function
#         result = func(*args, **kwargs)

#         # Get the captured print output
#         captured_output = sys.stdout.getvalue()

#         # Restore the original stdout
#         sys.stdout = original_stdout

#         # Return the result and the captured output
#         return result, captured_output

#     return wrapper 


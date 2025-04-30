# Market Data Platform

The Market Data Platform is an **online Integrated Development Environment (IDE)** designed to create, execute, and analyze custom financial indicators from market data. It leverages Celery and Dask for efficient parallel processing, enabling scalable and high-performance computation.

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/MDP_UI.png?raw=true)

## architecture

The platform follows a modular architecture, integrating a user-friendly front-end, a robust back-end server, and distributed task processing. The diagram below illustrates the system’s components and data flow:

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/architecture.png?raw=true)

## Input

The platform accepts two primary inputs from users: **data metadata** and **custom code**. These inputs are sent to the server for processing.

### 1.Data

Users specify metadata to define the data sources or parameters for financial indicators. The UI displays the full data (or a preview) for user interaction and validation. To optimize performance, only the metadata is sent to the server, significantly reducing payload size. The UI allows intuitive setup, as shown below:

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/Setup_UI.png?raw=true)

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/Setup_result.png?raw=true)

### 2.code

Users write custom Python code to define financial indicators. The platform provides a code editor and logs execution details for debugging:
![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/Transform.png?raw=true)

Execution logs for user code:

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/log.png?raw=true)

## Output

The server retrieves data from the database based on the provided metadata and executes the user’s code using Python’s `exec()` function. The results are returned to the user, typically as computed financial indicators or visualizations.

Example of code execution:

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/exec.png?raw=true)

Sample output of computed results:

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/Extract_result.png?raw=true)

## Parallel computing with celery and dask

To handle large-scale data processing, the platform uses **Celery** for distributed task queuing and **Dask** for parallel computation. This architecture ensures efficient execution of user code across multiple workers, optimizing performance for complex financial computations.

Celery task distribution:

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/Celery.png?raw=true)

Dask parallel processing:

![plot](https://github.com/quachthetruong/MDP_v2/blob/main/asset/Dask_full.png?raw=true)

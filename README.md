# Freactor

Freactor is a lightweight flow-control framework for Python.
It provides a simple way to define task pipelines using reducers and step transitions, supporting both synchronous and asynchronous (asyncio) execution.

# Features

Task orchestration with clear step transitions (SUCCESS, FAILURE, RETRY, ABORT).

Reducer decorators with automatic retry/delay logic.

Async support: AsyncFreactor + async_freducer for coroutine-based workflows.

Logging with task IDs for easy observability.

High performance:

~20–35k async tasks/sec per process (I/O bound).

Supports 1M+ reducer steps per process in a single event loop.

Minimal dependencies, works with Python 3.8+.

# Installation

```
pip install freactor
```

Or install in development mode:
```
git clone https://github.com/Pro-YY/freactor.git
cd freactor
pip install -e .
```


# Quick Start
## 1. Define Reducers
```
import asyncio
import logging
from freactor import StatusCode, async_freducer

log = logging.getLogger(__name__)

SUCCESS = StatusCode.SUCCESS

@async_freducer(retry=3, delay=1)
async def step1(data):
    log.info(f"[task {data['_task_id']}] step1 running...")
    await asyncio.sleep(1)  # simulate async workload
    return SUCCESS, {"step1": True}, "done"
```

## 2. Configure Task Flow
```
TASK_CONFIG = {
    "example_task": {
        "init_step": ("example_reducers", "step1"),
        "table": {
            ("example_reducers", "step1"): {SUCCESS: None}
        },
    }
}
```

## 3. Run Tasks with AsyncFreactor
```
import asyncio
from freactor import AsyncFreactor

async def main():
    f = AsyncFreactor(
        {"task_config": TASK_CONFIG, "import_reducer_prefix": "example_reducers."},
        num_actors=8,
    )
    await f.run_task("example_task", {"param": "demo"})
    await f.run_forever()

asyncio.run(main())
```


# Benchmarks

AsyncFreactor:

100k tasks in ~4.3s (≈22k tasks/s).

1M tasks in ~45s on a 10-core machine.

Multiprocessing scaling:
Combine multiple processes (each with its own actors) or connect pods via Redis Streams / RabbitMQ for horizontal scalability.

# License

[MIT]
This project is licensed under the [MIT License](./LICENSE).

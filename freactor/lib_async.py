import asyncio
from importlib import import_module
from random import randint
import logging

log = logging.getLogger(__name__)


class StatusCode:
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    ABORT = "ABORT"


def freducer(retry=0, delay=0):
    def deco(f):
        if asyncio.iscoroutinefunction(f):
            async def async_wrapper(t_data):
                try:
                    return await f(t_data)
                except Exception as e:
                    log.info(e)
                    k = "_general_retried_" + f.__name__
                    retried = t_data.get(k, 0) + 1
                    result = {k: retried}
                    if retried > retry:
                        return StatusCode.FAILURE, {}, f"{f.__name__} failed after {retry} retries: {e}"
                    else:
                        return StatusCode.RETRY, result, "async general-retry", delay
            return async_wrapper
        else:
            def sync_wrapper(t_data):
                try:
                    return f(t_data)
                except Exception as e:
                    log.info(e)
                    k = "_general_retried_" + f.__name__
                    retried = t_data.get(k, 0) + 1
                    result = {k: retried}
                    if retried > retry:
                        return StatusCode.FAILURE, {}, f"{f.__name__} failed after {retry} retries: {e}"
                    else:
                        return StatusCode.RETRY, result, "sync general-retry", delay
            return sync_wrapper
    return deco


class AsyncActor:
    def __init__(self, queue, result_queue, tasks, task_config, import_reducer_prefix="", sem=None):
        self.queue = queue
        self.result_queue = result_queue
        self.tasks = tasks
        self.task_config = task_config
        self.import_reducer_prefix = import_reducer_prefix
        self.sem = sem

    async def _exec_reducer(self, task_id, module_name, reducer_name, args):
        try:
            module = import_module(self.import_reducer_prefix + module_name)
            method = getattr(module, reducer_name)

            t_data = args[0]
            if isinstance(t_data, dict):
                t_data["_task_id"] = task_id

            result = method(t_data)
            if asyncio.iscoroutine(result):
                result = await result

            assert len(result) >= 3
            code, result_data, message = result[:3]
            delay = result[3] if len(result) == 4 else None
            return {"code": code, "result": result_data, "message": message, "delay": delay}
        except Exception as e:
            log.exception(f"[task {task_id}] Reducer runtime error in {module_name}.{reducer_name}")
            return {"code": -1, "result": {}, "message": str(e)}

    async def _run_with_limit(self, task_id, module_name, reducer_name, args):
        if self.sem is None:
            return await self._exec_reducer(task_id, module_name, reducer_name, args)
        async with self.sem:
            return await self._exec_reducer(task_id, module_name, reducer_name, args)

    async def run(self):
        while True:
            msg = await self.queue.get()
            task_id = msg["task_id"]
            t = self.tasks[task_id]

            if msg["type"] == "TASK_INIT":
                log.debug(f"[task {task_id}] TASK_INIT")
                t["data"].update(t["params"])
                t["path"].append("INIT")

                step = self.task_config[t["name"]]["init_step"]
                t["next_step"] = step
                fut = asyncio.create_task(self._run_with_limit(task_id, step[0], step[1], (t["data"].copy(),)))
                await self.result_queue.put((task_id, step, fut))

            elif msg["type"] == "TASK_EXEC":
                step = msg["step"]
                fut = asyncio.create_task(self._run_with_limit(task_id, step[0], step[1], msg["args"]))
                await self.result_queue.put((task_id, step, fut))

            elif msg["type"] == "TASK_NEXT":
                output = msg["output"]
                code = output["code"]
                t["data"].update(output["result"])

                prev_step = t["next_step"]
                if prev_step != t["path"][-1]:
                    t["path"].append(prev_step)

                table = self.task_config[t["name"]]["table"]
                next_step = table[prev_step].get(code)
                if next_step:
                    t["next_step"] = next_step
                    async def delayed_exec():
                        if output.get("delay"):
                            await asyncio.sleep(output["delay"])
                        return await self._run_with_limit(task_id, next_step[0], next_step[1], (t["data"].copy(),))
                    fut = asyncio.create_task(delayed_exec())
                    await self.result_queue.put((task_id, next_step, fut))
                else:
                    t["path"].append("DONE")
                    log.info(f"Task {task_id} done: path={t['path']} data={t['data']}")
                    del self.tasks[task_id]

            elif msg["type"] == "TASK_ERROR":
                # FIXME 'NoneType' object is not subscriptable, check reducers
                log.info(f"Task {task_id} error: {msg['output']['message']}")
                log.info(self.tasks[task_id]["path"])
                del self.tasks[task_id]

            self.queue.task_done()


class Collector:
    def __init__(self, result_queue, queue):
        self.result_queue = result_queue
        self.queue = queue

    async def run(self):
        while True:
            task_id, step, fut = await self.result_queue.get()
            try:
                output = await fut
                if output["code"] == -1:
                    await self.queue.put({"type": "TASK_ERROR", "task_id": task_id, "output": output})
                else:
                    await self.queue.put({"type": "TASK_NEXT", "task_id": task_id, "output": output})
            except Exception as e:
                await self.queue.put({
                    "type": "TASK_ERROR",
                    "task_id": task_id,
                    "output": {"code": -1, "result": {}, "message": str(e)},
                })
            finally:
                self.result_queue.task_done()


class AsyncFreactor:
    def __init__(
        self,
        config,
        num_actors=10,
        loop=None,
        *,
        max_queue_size=10_000,
        max_result_queue_size=10_000,
        max_inflight_reducers=1000,
        num_collectors=10,
    ):
        self._task_config = config.get("task_config")
        self._import_reducer_prefix = config.get("import_reducer_prefix", "")
        self._tasks = {}
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._result_queue = asyncio.Queue(maxsize=max_result_queue_size)
        self._actors = []
        self._num_actors = num_actors
        self._loop = loop or asyncio.get_event_loop()
        self._sem = asyncio.Semaphore(max_inflight_reducers)

        for _ in range(self._num_actors):
            actor = AsyncActor(self._queue, self._result_queue, self._tasks, self._task_config, self._import_reducer_prefix, sem=self._sem)
            self._actors.append(self._loop.create_task(actor.run()))

        self._collectors = [
            self._loop.create_task(Collector(self._result_queue, self._queue).run())
            for _ in range(num_collectors)
        ]

    async def run_forever(self):
        while True:
            await asyncio.sleep(3600)

    async def run_task(self, name, params, delay=None, *, block=True):
        task_id = f"t_{randint(0, 1e12)}"
        self._tasks[task_id] = {
            "task_id": task_id,
            "name": name,
            "params": params,
            "path": [],
            "data": {},
        }
        msg = {"type": "TASK_INIT", "task_id": task_id, "delay": delay}
        if block:
            await self._queue.put(msg)
        else:
            self._queue.put_nowait(msg)
        return task_id

    async def stop(self):
        await self._queue.join()
        await self._result_queue.join()
        for t in self._actors + self._collectors:
            t.cancel()
        await asyncio.gather(*self._actors, *self._collectors, return_exceptions=True)

import asyncio
import logging

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(filename)s:%(lineno)d] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# logging.getLogger("freactor").setLevel(logging.INFO)


from freactor import AsyncFreactor, StatusCode

SUCCESS = StatusCode.SUCCESS
FAILURE = StatusCode.FAILURE
RETRY = StatusCode.RETRY
ABORT = StatusCode.ABORT

TASK_CONFIG = {
    'example_task_1': {
        'init_step': ('basic', 's1'),
        'table': {
            ('basic', 's1'): {
                SUCCESS: ('basic', 's2'),
                FAILURE: None,
                RETRY: ('basic', 's1'),
                ABORT: None,
            },
            ('basic', 's2'): {
                SUCCESS: None,
                FAILURE: None,
                RETRY: ('basic', 's2'),
                ABORT: None,
            },
        }
    },
    'example_task_3': {
        'init_step': ('basic_async', 's1'),
        'table': {
            ('basic_async', 's1'): {
                SUCCESS: ('basic_async', 's3'),
                FAILURE: None,
                RETRY: ('basic_async', 's1'),
                ABORT: None,
            },
            ('basic_async', 's2'): {
                SUCCESS: None,
                FAILURE: None,
                RETRY: ('basic_async', 's2'),
                ABORT: None,
            },
            ('basic_async', 's3'): {
                SUCCESS: ('basic_async', 's5'),
                FAILURE: ('basic_async', 's2'),
                RETRY: ('basic_async', 's3'),
                ABORT: None,
            },
            ('basic_async', 's4'): {
                SUCCESS: ('basic_async', 's2'),
                FAILURE: None,
                RETRY: ('basic_async', 's4'),
                ABORT: None,
            },
            ('basic_async', 's5'): {
                SUCCESS: None,
                FAILURE: ('basic_async', 's4'),
                RETRY: ('basic_async', 's5'),
                ABORT: None,
            }
        }
    },
    'example_task_4': {
        'init_step': ('basic_async', 'call_http_api'),
        'table': {
            ('basic_async', 'call_http_api'): {
                SUCCESS: None,
                FAILURE: None,
                RETRY: ('basic_async', 'call_http_api'),
                ABORT: None,
            }
        }
    }
}


async def main():
    log.debug('Hello, Freactor!')

    f = AsyncFreactor({
        'task_config': TASK_CONFIG,
        'import_reducer_prefix': 'example-reducers.',
    })

    try:
        await f.run_task('example_task_3', {"task": "demo"})
        # await f.run_task('example_task_4', {"task": "demo"})

        # NOTE: when 1K, cpu burst 100%, tasks emerged then executed later
        # for i in range(9):
        #    await f.run_task('example_task_3', {"my_task_name": str(i)})
        #    await f.run_task('example_task_4', {"my_task_name": str(i)})

        await f.run_forever()
 
    except asyncio.CancelledError:
        pass
    finally:
        await f.stop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Ctrl+C pressed, shutting down...")

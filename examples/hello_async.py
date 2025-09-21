import asyncio
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

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
    }
}


async def main():
    log.debug('Hello, Freactor!')

    f = AsyncFreactor({
        'task_config': TASK_CONFIG,
        'import_reducer_prefix': 'example-reducers.',
    })

    await f.run_task('example_task_3', {"task": "demo"})

    for i in range(9):
       await f.run_task('example_task_3', {"my_task_name": str(i)})

    await f.run_forever()

if __name__ == '__main__':
    asyncio.run(main())

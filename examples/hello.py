import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

from freactor import Freactor, StatusCode

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
    'example_task_2': {
        'init_step': ('basic', 's1'),
        'table': {
            ('basic', 's1'): {
                SUCCESS: ('basic', 's3'),
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
            ('basic', 's3'): {
                SUCCESS: ('basic', 's5'),
                FAILURE: ('basic', 's2'),
                RETRY: ('basic', 's3'),
                ABORT: None,
            },
            ('basic', 's4'): {
                SUCCESS: ('basic', 's2'),
                FAILURE: None,
                RETRY: ('basic', 's4'),
                ABORT: None,
            },
            ('basic', 's5'): {
                SUCCESS: None,
                FAILURE: ('basic', 's4'),
                RETRY: ('basic', 's5'),
                ABORT: None,
            }
        }
    }
}


def main():
    log.debug('Hello, Freactor!')

    f = Freactor({
        'task_config': TASK_CONFIG,
        'import_reducer_prefix': 'example-reducers.',
        'threads': 4,
    })

    f.run_task('example_task_2', {})

    while True: pass

if __name__ == '__main__':
    main()

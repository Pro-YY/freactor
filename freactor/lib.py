from threading import Thread
from queue import Queue
from importlib import import_module
import select
from random import randint
from time import time
from functools import wraps

import logging
log = logging.getLogger(__name__)

# Linux event_fd/timer_fd
import ctypes, os
libc = ctypes.CDLL('libc.so.6')

CLOCK_MONOTONIC = 1

class timespec(ctypes.Structure):
    _fields_ = [
        ('tv_sec', ctypes.c_long),
        ('tv_nsec', ctypes.c_long),
    ]


class itimerspec(ctypes.Structure):
    _fields_ = [
        ('it_interval', timespec),
        ('it_value', timespec),
    ]


class TimerFd(object):
    def __init__(self):
        # no os.O_CLOEXEC until python 3.3
        ret = libc.timerfd_create(CLOCK_MONOTONIC, os.O_NONBLOCK)
        if ret < 0: raise Exception('timerfd create failed')
        else: self._fd = ret

    def get_fd(self):
        return self._fd

    def close(self):
        try:
            if self._fd:
                os.close(self._fd)
        except:
            pass
        self._fd = None

    def __del__(self):
        self.close()

    def set_time(self, value, interval=0):
        new_value = itimerspec()
        new_value.it_value.tv_sec = int(value)
        new_value.it_value.tv_nsec = int((value - int(value))*1e9)
        new_value.it_interval.tv_sec = int(interval)
        new_value.it_interval.tv_nsec = int((interval - int(interval))*1e9)
        old_value = itimerspec()
        ret = libc.timerfd_settime(self._fd, 0, ctypes.pointer(new_value), ctypes.pointer(old_value))
        if ret < 0: raise Exception('timerfd settime failed')

    def read(self):
        try:
            ret = os.read(self._fd, 8) # uint64_t
            return ret
        except OSError as e:
            # EAGAIN (errno 11 will get if non-blocking read)
            raise


class EventFd(object):
    def __init__(self, value=0):
        # no os.O_CLOEXEC until python 3.3
        ret = libc.eventfd(value, os.O_NONBLOCK)
        if ret < 0: raise Exception('eventfd create failed')
        else: self._fd = ret

    def get_fd(self):
        return self._fd

    def close(self):
        try:
            if self._fd:
                os.close(self._fd)
        except:
            pass
        self._fd = None

    def __del__(self):
        self.close()

    def read(self):
        try:
            ret = os.read(self._fd, 8) # uint64_t
            return ret
        except OSError as e:
            # EAGAIN (errno 11 will get if non-blocking read)
            raise

    def write(self, value=0xffffffff):
        try:
            ret = os.write(self._fd, str(value).encode()) # should use byte in py3
            return ret
        except OSError as e:
            # EAGAIN (errno 11 will get if non-blocking read)
            raise


# Freactor Implementation
# update output, find next step, queue consumer, event producer
class Conductor(Thread):
    def __init__(self, config):
        super(Conductor, self).__init__()
        self.daemon = True
        self._queue = config['queue']
        self._epoll = config['epoll']
        self._events = config['events']
        self._task_config = config['task_config']
        self._tasks = config['tasks']

    def _attach_event(self, msg):
        def _add_event(fd, msg):
            self._events[fd.get_fd()] = {
                'fd': fd,
                'task_id': msg['task_id'],
                'module': msg['module'],
                'reducer': msg['reducer'],
                'args': msg['args'],
            }
        expire = msg.get('delay')
        if expire is not None:
            tfd = TimerFd()
            v = max(1e-3, expire)
            self._epoll.register(tfd.get_fd(), select.EPOLLIN | select.EPOLLET)
            _add_event(tfd, msg)
            tfd.set_time(v)
        else:
            efd = EventFd()
            self._epoll.register(efd.get_fd(), select.EPOLLIN | select.EPOLLET)
            _add_event(efd, msg)
            efd.write()

    def run(self):
        while True:
            queue_msg = self._queue.get()
            assert queue_msg['type'] in ['TASK_INIT', 'TASK_NEXT', 'TASK_ERROR']
            task_id = queue_msg['task_id']
            try:
                if queue_msg['type'] == 'TASK_INIT':
                    log.debug('TASK_INIT %s got!' % task_id)
                    delay = queue_msg.get('delay')
                    # update task data
                    self._tasks[task_id]['data'].update(self._tasks[task_id]['params'])
                    # update path
                    self._tasks[task_id]['path'].append('INIT')
                    # find next step
                    next_step = self._task_config[self._tasks[task_id]['name']]['init_step']
                    assert next_step
                    self._tasks[task_id]['next_step'] = next_step
                    # assemble event message
                    t_data = self._tasks[task_id]['data'].copy()
                    event_msg = {'task_id': task_id, 'module': next_step[0], 'reducer': next_step[1], 'args': (t_data,), 'delay': delay}
                    self._attach_event(event_msg)
                elif queue_msg['type'] == 'TASK_NEXT':
                    output = queue_msg['output']
                    code = output['code']
                    result = output['result']
                    message = output['message']
                    delay = output.get('delay')
                    log.debug('TASK_NEXT %s got!' % task_id)
                    log.debug(output)
                    # update task data
                    self._tasks[task_id]['data'].update(result)
                    # update path
                    prev_step = self._tasks[task_id]['next_step']
                    if prev_step != self._tasks[task_id]['path'][-1]: # do not record retry step in path
                        self._tasks[task_id]['path'].append(prev_step)
                    # find next step
                    table = self._task_config[self._tasks[task_id]['name']]['table']
                    next_step = table[prev_step][code]  # the most significant line
                    log.debug('%s next step: %s' % (task_id, str(next_step)))
                    if next_step:
                        self._tasks[task_id]['next_step'] = next_step
                        # assemble event message
                        t_data = self._tasks[task_id]['data'].copy()
                        event_msg = {'task_id': task_id, 'module': next_step[0], 'reducer': next_step[1], 'args': (t_data,), 'delay': delay}
                        self._attach_event(event_msg)
                    else:
                        self._tasks[task_id]['path'].append('DONE')
                        log.debug('task %s done!' % task_id)
                        log.info(self._tasks[task_id]['path'])
                        log.debug(message)
                        log.debug(self._tasks[task_id]['data'])
                        del self._tasks[task_id]
                else: # 'TASK_ERROR' unexpected raise
                    output = queue_msg['output']
                    message = output['message']
                    log.debug('task %s error: %s' % (task_id, message))
                    log.info(self._tasks[task_id]['path'])
                    log.debug(message)
                    log.debug(self._tasks[task_id]['data'])
                    del self._tasks[task_id]
            except Exception as e:
                import traceback
                for l in traceback.format_exc().splitlines(): log.error(l)
            finally:
                self._queue.task_done()


# exec steps, queue producer, event consumer
class Actor(Thread):
    def __init__(self, config):
        super(Actor, self).__init__()
        self.daemon = True  # exit when main process exit
        self._queue = config['queue']
        self._epoll = config['epoll']
        self._events = config['events']
        self._import_reducer_prefix = config['import_reducer_prefix']

    def _exec_reducer(self, data):
        def _unpack(*args): return tuple(args)
        try:
            module = import_module(self._import_reducer_prefix + data['module'])
            method = getattr(module, data['reducer'])
            args = data['args']
            # TODO timeout
            result_tuple = _unpack(*method(*args))
            assert len(result_tuple) >=3
            code, result, message = result_tuple[:3]
            delay = None
            delay = result_tuple[3] if len(result_tuple) == 4 else None
            output = { 'code': code, 'result': result, 'message': message, 'delay': delay}
        except Exception as e:
            log.error(' runtime error!')
            log.error(e)
            import traceback
            for l in traceback.format_exc().splitlines(): log.error(l)
            output = { 'code': -1, 'message': 'runtime error %s' % repr(e.args[0]) }
        finally:
            return output

    def run(self):
        while True:
            events = self._epoll.poll(1)
            for fd, event in events:
                if fd in self._events and event & select.EPOLLIN:
                    task_id = self._events[fd]['task_id']
                    output = self._exec_reducer(self._events[fd])
                    self._events[fd]['fd'].read()
                    self._events.pop(fd)
                    # output result state to queue
                    if output['code'] == -1:
                        queue_msg = {'type': 'TASK_ERROR', 'task_id': task_id, 'output': output}
                    else:
                        queue_msg = {'type': 'TASK_NEXT', 'task_id': task_id, 'output': output}
                    self._queue.put(queue_msg)
            # can do some thing after polling


# init threads/queue/epoll, and record tasks
class Freactor(object):
    def __init__(self, config):
        super(Freactor, self).__init__()
        self._task_config = config.get('task_config')
        self._import_reducer_prefix = config.get('import_reducer_prefix', '')
        self._num_actor_threads = config.get('threads', 4)
        self._queue = Queue()
        self._epoll = select.epoll()
        self._events = {}
        self._tasks = {}    # current running tasks
        self._conductor = Conductor({
            'queue': self._queue,
            'epoll': self._epoll,
            'events': self._events,
            'task_config': self._task_config,
            'tasks': self._tasks,
        })
        self._conductor.start()
        self._actors = []
        for _ in range(self._num_actor_threads):
            a = Actor({'queue': self._queue, 'epoll': self._epoll, 'events': self._events, 'import_reducer_prefix': self._import_reducer_prefix})
            a.start()
            self._actors.append(a)
        log.debug('freactor init done')
        log.debug(self._task_config)

    def run_task(self, name, params, delay=None):
        task_id = "t_%s" % randint(0, 1e8)
        self._tasks[task_id] = {
            'task_id': task_id,
            'name': name,
            'params': params,
            'path': [],
            'data': {},
        }
        msg = {'type': 'TASK_INIT', 'task_id': task_id, 'delay': delay}
        self._queue.put(msg)


# For usual and best practice, we define the most common status transfer code as below:
class StatusCode(object):
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    RETRY = 'RETRY'
    ABORT = 'ABORT'
    # codes larger than 3 are for more advanced business-specific state-transfer scenarios.

# Note: reducers should add this decorator for safety and convenience.
# This decorator treats unhandled exceptions as general failure(code 1) and perform (optionally) retries.
# Business Reducers now can handle failure/retry logic by self, as well as by doing `raise`.
# To use this decorator, business code should conform to the code convention above.
def freducer(retry=3, delay=3):
    def deco(f):
        @wraps(f)
        def wrapper(t_data):
            try:
                return f(t_data)
            except Exception as e:
                log.error(e)
                k = '_general_retried_' + f.__name__
                retried = t_data.get(k) + 1 if t_data.get(k) != None else 1
                result = {}
                result[k] = retried
                if retried > retry:
                    return StatusCode.FAILURE, {}, '%s general-failure after genearl-retry %s times: %s' % (f.__name__, retry, e)
                else:
                    return StatusCode.RETRY, result, 'f1 general-retry', delay
        return wrapper
    return deco


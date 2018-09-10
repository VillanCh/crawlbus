#!/usr/bin/env python3
import uuid
import unittest
import threading
import traceback
from threading import Thread, Event
from queue import Queue, Empty


class _Task(object):

    def __init__(self, func, args: list, kwargs: dict, id=None):
        if not callable(func):
            raise Exception("func: {} is not callable".format(func))

        self._id = id if id else uuid.uuid4().hex
        self.func = func
        self.args = args
        self.kwargs = kwargs


class _Result(object):

    def __init__(self, task, result=None, traceback=None):
        self.task = task
        self.result = result
        self.traceback = traceback


class _Labor(Thread):

    def __init__(self, taskq, resultq, name=None):
        self.taskq = taskq
        self.resultq = resultq
        name = name if name else "_labor-{}".format(uuid.uuid4())
        Thread.__init__(self, name=name)
        self.daemon = True
        self.labor_is_working = False
        self.is_executing_task = Event()

    def run(self):
        self.labor_is_working = True
        while self.labor_is_working:
            try:
                self._run()
            except Exception as e:
                print('thread is closed by accident: {}'.format(e))

    def _run(self):
        while self.labor_is_working:
            try:
                _task = self.taskq.get(timeout=1)
                self.is_executing_task.set()
            except Empty:
                continue

            try:
                # assert isinstance(_task, _Task)
                result = _task.func(*_task.args, **_task.kwargs)
                trackinfo = None
            except:
                try:
                    trackinfo = traceback.format_exc()
                except:
                    trackinfo = "UNKNOW ERROR!"
                result = None

            self.resultq.put(_Result(
                _task, result, trackinfo
            ))
            self.is_executing_task.clear()

    def prepare_stop(self):
        self.labor_is_working = False

    def stop(self):
        self.join()


class Pool(object):

    def __init__(self, size=20, _laborcls=_Labor, *args, **kwargs):
        self.size = size
        self.mainthread = Thread(name="pool-main", target=self._main)
        self.mainthread.daemon = True
        self._threads = {}
        self._working = False
        self._dispatcher_queue = Queue()
        self.task_queue = Queue()
        self.result_queue = Queue()
        self._laborcls = _laborcls

    def start(self):
        [self._new_labor() for _ in range(self.size)]
        self.mainthread.start()

    def _main(self):
        self._working = True
        while self._working:
            try:
                _ret = self._dispatcher_queue.get(timeout=1)
                if isinstance(_ret, _Task):
                    self.task_queue.put(_ret, block=False)
                elif isinstance(_ret, _Result):
                    self.result_queue.put(_ret, block=False)
            except Empty:
                pass

    def execute(self, func, args=(), kwargs={}, id=None):
        _t = _Task(func, args, kwargs, id)
        self._dispatcher_queue.put(_t)

    def stop(self):
        self._working = False
        self.mainthread.join()
        [i.prepare_stop() for i in self._threads.values()]
        [i.stop() for i in self._threads.values()]

    def _new_labor(self):
        lname = uuid.uuid4()
        labor = self._laborcls(self.task_queue, self.result_queue, lname)
        labor.daemon = True
        self._threads[lname] = labor
        labor.start()

    def is_working(self):
        return self._working

    def all_is_idle(self):
        threads = [labor for labor in self._threads.values(
        ) if labor.is_executing_task.is_set()]
        if threads:
            return False
        else:
            return True

    def all_is_finished(self):
        """"""
        return self.task_queue.empty() and self._dispatcher_queue.empty() \
            and self.result_queue.empty() and self.all_is_idle()

    def wait_until_all_is_finished(self):
        """"""
        count = 0
        while True:
            if self.all_is_finished():
                count += 1
            else:
                count = 0

            if count > 2:
                break


def test(a, b, c):
    print(a, b, c)


class PoolTester(unittest.TestCase):
    """"""

    def test_pool(self):
        """"""
        pool = Pool(size=10)
        pool.start()

        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})
        pool.execute(test, (1, 2), {"c": "123123123"})

        pool.stop()


if __name__ == '__main__':
    unittest.main()

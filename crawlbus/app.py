#!/usr/bin/env python3
# coding:utf-8
import time
import functools
import logging
import threading
import uuid
import queue
from . import pool
from . import outils
from . import task_context
from . import knowledgebase

logger = outils.get_logger("crawlbus")
logging.root.addHandler(hdlr=logging.NullHandler())

DEFAULT_CONFIG = {
    "default_request_params": {
        "method": "GET",

    },
    "options": {
        "poolsize": 20
    }
}


class _CQueue(object):
    """"""

    def __init__(self, name, pool: pool.Pool, size=0):
        """Constructor"""
        self.name = name
        self.pool = pool
        self._q = queue.Queue(size)
        self._handlers = []

    def put_when_existed_handler(self, item):
        for handler in self._handlers:
            self.pool.execute(handler, (item, ))

    def add_handler(self, handler):
        if handler in self._handlers:
            logger.info('add repeat handler:{} for {}'.format(handler, self.name))
        else:
            self._handlers.append(handler)


class QueueManager(object):
    """"""

    def __init__(self, poolsize=5):
        """Constructor"""
        # start pool
        self._pool = pool.Pool(poolsize)
        self._pool.start()
        self._pool.result_queue.put = self._null

        self.queue_new_url = _CQueue(knowledgebase.QUEUE_NEW_URL, self._pool)
        self.queues = {
            knowledgebase.QUEUE_NEW_URL: self.queue_new_url
        }

    def get_queue_by_name(self, queue_name) -> _CQueue:
        return self.queues[queue_name]

    def _null(self, item):
        pass
        #rz = item.traceback
        # print()


class Crawler:

    def __init__(self, url=None, config={}):
        self.start_url = url
        self.config = DEFAULT_CONFIG
        self.config.update(config)

        self.options = self.config.get("options", {})

        self.pool = pool.Pool(self.options.get('poolsize', 20))
        self.pool.start()

        self.contexts = {}

        # queue manager
        self.queue_manager = QueueManager(self.options.get("callback_poolsize", 5))

    def start(self):
        """"""
        _id = uuid.uuid4().hex
        context = self.create_task_context(self.start_url, self.config.get(
            "default_request_params"), id=_id)
        if _id not in self.contexts:
            self.contexts[_id] = context
        else:
            raise ValueError("context: {} is existed.")

        # start task
        self.start_task(context)

    def create_task_context(self, start_url, request_params, id=None):
        """"""
        context = task_context.TaskContext(
            callback_manager=self.queue_manager,
            pool=self.pool,
            id=id, url=start_url, request_params=request_params)
        context.options.update(self.config)
        return context

    def start_task(self, context: task_context.TaskContext):
        """"""
        for req in context.emit():
            self.pool.execute(context.request, (req, ))

    def wait_until_finished(self):
        """"""
        self._collect_result()

    def _collect_result(self):
        """"""
        while not self.pool.all_is_finished():
            try:
                result = self.pool.result_queue.get(timeout=3)
            except queue.Empty:
                continue

            if not isinstance(result, pool._Result):
                continue

            if result.traceback:
                logger.warn(result.traceback)

            res = result.result
            cid = res["id"]
            if cid not in self.contexts:
                logger.warn("the task_id:{} is not existed.".format(cid))
                continue

            context = self.contexts[cid]
            context.feedback(res)

    def add_handler_for_queue_name(self, queue_name, handler):
        cq = self.queue_manager.get_queue_by_name(queue_name)
        cq.add_handler(handler)

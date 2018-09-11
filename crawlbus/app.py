#!/usr/bin/env python3
# coding:utf-8
import logging
import uuid
import queue
from . import pool
from . import outils
from . import task_context


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


class CrawlBus:

    def __init__(self, url=None, config={}):
        self.start_url = url
        self.config = DEFAULT_CONFIG
        self.config.update(config)

        self.options = self.config.get("options", {})

        self.pool = pool.Pool(self.options.get('poolsize'))
        self.pool.start()

        self.contexts = {}

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
            id=id, url=start_url, **request_params)
        context.bind_pool(self.pool)
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






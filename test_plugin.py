#!/usr/bin/env python3
# coding:utf-8
import crawlbus
import logging
from crawlbus.utils import outils

outils.get_logger("crawlbus").setLevel(logging.DEBUG)

logger = outils.get_logger("crawlbus")


def on_new_url(*vargs, **kwargs):
    task_context, url = vargs[0]
    #logger.debug("on new url: {}".format(url))


def on_new_req(*vargs, **kwargs):
    _, req = vargs[0]
    # print("on new req: {}".format(req))

def main():
    app = crawlbus.Crawler('https://www.163.com/')
    app.start()

    bus = crawlbus.CrawlBus(crawler=app)
    bus.link_queue_to_handler("new_url", on_new_url)
    bus.link_queue_to_handler("new_req", on_new_req)

    app.wait_until_finished()

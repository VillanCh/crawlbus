#!/usr/bin/env python3
# coding:utf-8
import crawlbus
import logging
from crawlbus import outils


def on_new_url(*vargs, **kwargs):
    print(vargs, kwargs)


def on_new_req():
    pass


outils.get_logger("crawlbus").setLevel(logging.INFO)

app = crawlbus.Crawler('http://127.0.0.1:8080')
app.start()

bus = crawlbus.CrawlBus(crawler=app)
bus.link_queue_to_handler("new_url", on_new_url)

app.wait_until_finished()

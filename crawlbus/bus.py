#!/usr/bin/env python3
# coding:utf-8
from .app import Crawler
from . import knowledgebase


class CrawlBus:
    """"""

    def __init__(self, crawler: Crawler):
        """Constructor"""
        if not crawler:
            raise ValueError("not a valid crawler")
        self.crawler = crawler

    def link_queue_to_handler(self, queue_name, handler):
        if queue_name not in knowledgebase.BUS_ENTRIES:
            raise ValueError("the {} is not in BusEntries:{}".format(queue_name,
                                                                     knowledgebase.BUS_ENTRIES))
        self.crawler.add_handler_for_queue_name(queue_name, handler)

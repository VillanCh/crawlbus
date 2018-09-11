#!/usr/bin/env python3
# coding:utf-8
import crawlbus
from crawlbus import outils
import logging

logger = outils.get_logger("crawlbus")
logger.setLevel(logging.INFO)

bus = crawlbus.CrawlBus(url="http://www.leavesongs.com", config={})

bus.start()

bus.wait_until_finished()

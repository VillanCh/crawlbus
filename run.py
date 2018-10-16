#!/usr/bin/env python3
# coding:utf-8
import crawlbus
from crawlbus.utils import outils
import logging

logger = outils.get_logger("crawlbus")
logger.setLevel(logging.INFO)

bus = crawlbus.Crawler(url="http://127.0.0.1:8080", config={})

bus.start()

bus.wait_until_finished()

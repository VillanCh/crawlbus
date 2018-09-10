#!/usr/bin/env python3
# coding:utf-8
import crawlbus

bus = crawlbus.CrawlBus(url="http://www.leavesongs.com", config={})

bus.start()

bus.wait_until_finished()

#!/usr/bin/env python3
# coding:utf-8
import unittest
import time

from ..core import CrawlerPipelineHandler
from ..core import CrawlerPipeline
from ..core import CrawlerPipelineSummary


class CrawlerPipelineHandlerDemo(CrawlerPipelineHandler):

    def on_new_domain(self):
        pass


class PipelineTestCase(unittest.TestCase):

    def test_pipeline_basic(self):
        pipeline = CrawlerPipeline()
        pipeline.start(start_url="http://127.0.0.1:8080", method="GET",
                       headers=None, data=None, params=None, auth=None, cookies=None,)

        time.sleep(3)
        pipeline.wait_until_finished()

        self.assertIsInstance(pipeline.get_summary(), CrawlerPipelineSummary)

if __name__ == "__main__":
    unittest.main()
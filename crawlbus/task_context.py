#!/usr/bin/env python3
# coding:utf-8
import requests
import bs4

from . import reqfilter


class TaskContext:

    def __init__(self, id, method, url, headers=None, files=None, data=None, params=None, auth=None, cookies=None, hooks=None, json=None):
        self.start_req = requests.Request(method, url, headers, files, data,
                                          params, auth, cookies, hooks, json)
        self.session = requests.Session()
        self.pool = None
        self.id = id

        self.reqfilter = reqfilter.FakeStaticPathFilter()

    def emit(self):
        """"""
        yield self.start_req

    def request(self, req):
        """"""
        req = self.session.prepare_request(req)
        rsp = self.session.send(req)
        if not isinstance(rsp, requests.Response):
            raise ValueError(
                "response is invalid by requests: {}".format(rsp))

        return {
            "id": self.id,
            "raw": rsp.text
        }

    def bind_pool(self, pool):
        """"""
        self.pool = pool

    def feedback(self, res):
        """"""
        raw = res.get("raw")
        if not raw:
            return

        for url in self._find_all_urls(raw):
            if not self.continue_url():
                continue

    def _find_all_urls(self, raw):
        """"""
        print(raw)
        soup = bs4.BeautifulSoup(raw, 'html.parser')
        for atag in soup.find_all():
            pass

    def continue_url(self, url):
        """"""
        if not self.reqfilter.url_is_duplicate(url, mehtod="GET"):
            self.reqfilter.add_url(url, method="GET")
            return True
        else:
            return False









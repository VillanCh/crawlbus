#!/usr/bin/env python3
# coding:utf-8
import os
import requests
import threading
import bs4
from urllib.parse import urljoin, urlparse
from . import reqfilter
from . import outils

logger = outils.get_logger('crawlbus')

FILE_SUFFIX_BLACKLIST = [
    ".pdf", ".zip",
    ".docx", ".doc", ".ppt", "pptx",
    ".jpg", ".png", ".gif", ".jpeg"
    # ".js", ".css"
]


class TaskContext:

    def __init__(self, callback_manager, pool, id, url, request_params):
        self.start_req = requests.Request(url=url, **request_params)
        self.base_domain = urlparse(self.start_req.url).netloc
        self.session = requests.Session()
        self.pool = pool
        self.id = id

        self.callback_manager = callback_manager

        self.reqfilter = reqfilter.FakeStaticPathFilter(0)
        self.filterlock = threading.Lock()
        self.options = {}

    def emit(self):
        """"""
        yield self.start_req

    def request(self, req):
        """"""
        logger.info("request url: {}".format(req.url))
        req = self.session.prepare_request(req)
        rsp = self.session.send(req)
        if not isinstance(rsp, requests.Response):
            raise ValueError(
                "response is invalid by requests: {}".format(rsp))

        return {
            "id": self.id,  # keep it!
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
            if self.is_duplicate_url(url):
                continue

            logger.debug("find new url: {}".format(url))
            self.callback_manager.queue_new_url.put_when_existed_handler((self, url))

            if not self._forbid_by_policy(url):
                self.pool.execute(self.request,
                                  (self.build_request_from_url(url),))

    def _find_all_urls(self, raw):
        """"""
        soup = bs4.BeautifulSoup(raw, 'html.parser')

        def genurls():
            for atag in soup.find_all():
                _url = atag.attrs.get("href")
                if _url:
                    yield self._fix_url(_url)

                _url1 = atag.attrs.get("src")
                if _url1:
                    yield self._fix_url(_url1)

        for _url in genurls():
            if _url:
                yield _url

    def _fix_url(self, url):
        def basic():
            if url.startswith("javascript:"):
                return
            elif url.startswith("data:"):
                return
            elif url.startswith("ftp:"):
                return
            elif url.startswith("mailto:"):
                return
            elif url.startswith("http"):
                return url
            else:
                return urljoin(self.start_req.url, url)

        url = basic()
        if url and "#" in url:
            return url[:url.index("#")]
        else:
            return url

    def is_duplicate_url(self, url):
        """"""
        with self.filterlock:
            if not self.reqfilter.url_is_duplicate(url, method="GET"):
                self.reqfilter.add_url(url, method="GET")
                return False
            else:
                return True

    def _forbid_by_policy(self, url):
        urli = urlparse(url)

        # static file suffix filter
        _, _ext = os.path.splitext(urli.path)
        if _ext in FILE_SUFFIX_BLACKLIST:
            return True
            # if not urli.query:
                # return True
            # else:
                # return False

        # domain restriction
        if urli.netloc != self.base_domain:
            return True

        return False

    def build_request_from_url(self, url):
        return requests.Request(url=url,
                                **self.options.get("default_request_params", {}))



#!/usr/bin/env python3
# coding:utf-8
import os
import requests
import threading
import bs4
from urllib.parse import urljoin, urlparse
from crawlbus.utils import reqfilter, outils
from . import config

global_config = config.global_config

logger = outils.get_logger('crawlbus')


class TaskContext:

    def __init__(self, callback_manager, pool, id, url, request_params):
        self.start_req = requests.Request(url=url, **request_params)
        self.base_domain = urlparse(self.start_req.url).netloc
        self.session = requests.Session()
        self.pool = pool
        self.id = id

        self.callback_manager = callback_manager

        self.reqfilter = reqfilter.FakeStaticPathFilter(
            distance=global_config.url_simhash_distance,
            bloomfilter=None,
            filter_dothtml=global_config.filter_dothtml,
            ignore_param_value=global_config.ignore_param_value
        )
        self.filterlock = threading.Lock()

    def emit(self):
        """"""
        yield self.start_req

    def request(self, req):
        """"""
        logger.info("request url: {}".format(req.url))
        req = self.session.prepare_request(req)

        # req hook
        self.callback_manager.queue_new_req.put_when_existed_handler(
            (self, req))

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
            self.callback_manager.queue_new_url.put_when_existed_handler(
                (self, url))

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
        if not global_config.allow_fragment and url and "#" in url:
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
        if _ext in global_config.suffix_blacklist:
            if not global_config.allow_static_file_with_query:
                return True
            else:
                if not urli.query:
                    return True
                else:
                    return False

        # domain restriction
        if self._filter_by_domain(urli.netloc):
            return True

        return False

    def build_request_from_url(self, url):
        return requests.Request(url=url,
                                **global_config.default_request_params)

    def _filter_by_domain(self, domain):
        """"""
        if not global_config.domain_blacklist and not global_config.domain_whitelist:
            if self.base_domain != domain:
                return True

        if domain in global_config.domain_blacklist:
            return True

        for white in global_config.domain_whitelist:
            if "*." in white:
                base = white[white.index("*.") + 2:]
                return not domain.endswith(base)
            else:
                return white != domain

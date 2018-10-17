#!/usr/bin/env python3
# coding:utf-8
import os
import typing
import bs4
import queue
from urllib.parse import urljoin, urlparse
import requests
import traceback
import threading
from .config import CrawlerConfig
from .utils import outils, pool, reqfilter

logger = outils.get_logger("pipeline")
logger.setLevel("DEBUG")


class CrawlerPipelineHandler(object):

    def __init__(self, pipeline):
        self.pipeline: CrawlerPipeline = pipeline

    def extra_url_checker(self, url):
        return True

    def hook_before_preparing_request(self, request):
        return request

    def hook_before_sending_request(self, prepared_request):
        return prepared_request

    def on_new_domain(self, domain: str):
        pass

    def on_new_url(self, url: str):
        pass

    def on_new_prepared_request(self, request: requests.PreparedRequest):
        pass

    def on_pipeline_starting(self):
        pass

    def on_pipeline_finished(self):
        pass


class CrawlerPipelineSummary(typing.NamedTuple):
    current_finished_request_count: int
    current_started_request_count: int


def _null(*v, **kw):
    pass


class CrawlerPipeline(object):

    def __init__(self, config: CrawlerConfig = None, handler=None):
        self.config = config or CrawlerConfig()
        handler = handler or CrawlerPipelineHandler
        self.handlers: CrawlerPipelineHandler = handler(pipeline=self)

        # build worker pool and disable result queue
        self.worker_pool = pool.Pool(self.config.poolsize)
        self._old_execute = self.worker_pool.execute
        self.worker_pool.execute = self._fake_execute

        # dispatcher thread
        self.dispacher_thread = threading.Thread(target=self._pipeline_dispatcher)
        self.dispacher_thread.daemon = True

        self._have_started_event = threading.Event()

        self.session = requests.Session()
        self._init_cookie = None
        self._init_request = None
        self._init_domain = None
        self._init_netloc = None

        # set reqfilter
        self.request_filter = reqfilter.FakeStaticPathFilter(
            self.config.url_simhash_distance,
            None,
            self.config.filter_dothtml,
            self.config.ignore_param_value
        )

        # domain
        self.domains = set()

        # counter
        self._started = 0
        self._finished = 0

    def start(self, start_url, method="GET", headers=None, data=None,
              params=None, auth=None, cookies=None):
        if self._have_started_event.is_set():
            raise ValueError("the pipeline is started.")

        req = self.build_request(start_url, method, headers, data,
                                 params, auth, cookies)
        self._init_cookie = cookies
        self._init_request = req
        self._init_domain, self._init_netloc = self.__extract_domainNnetloc(req.url)

        self._have_started_event.set()
        self.worker_pool.start()
        self.dispacher_thread.start()

        self.worker_pool.execute(self.request, args=(req,))

        self.handlers.on_pipeline_starting()

    def get_summary(self):
        return CrawlerPipelineSummary(
            current_finished_request_count=self._finished,
            current_started_request_count=self._started,
        )

    def build_request(self, url, method="GET", headers=None, data=None, param=None,
                      auth=None, cookies=None):
        req = requests.Request(method, url, headers, data=data, params=param, auth=auth,
                               cookies=cookies)
        return req

    def request(self, request: requests.Request) -> typing.Tuple[requests.Response, requests.PreparedRequest]:
        if self.config.fixed_cookie and self._init_cookie:
            self.session.cookies = self._init_cookie

        logger.info("{} -> {}".format(request.method, request.url))

        request = self.handlers.hook_before_preparing_request(request)
        prepared_request = self.session.prepare_request(request)

        response: requests.Response = None
        try:
            prepared_request = self.handlers.hook_before_sending_request(prepared_request)
            self.handlers.on_new_prepared_request(prepared_request)
            response = self.session.send(prepared_request)
        except Exception:
            logger.debug(traceback.format_exc())

        return response, prepared_request

    def _pipeline_dispatcher(self):
        """
        Dispatch results from request.

        :return:
        """
        logger.info("dispatcher is running")
        while self._have_started_event.is_set():
            try:
                result = self.worker_pool.result_queue.get(timeout=2)
                logger.debug("got result from result queue: {}".format(result))

                # set finished
                self._finished += 1

            except queue.Empty:
                continue

            if result.result:
                rsp, req = result.result
            else:
                logger.debug("error in pipeline.request {}".format(
                    result.traceback
                ))
                continue

            response: requests.Response = rsp
            request: requests.PreparedRequest = req

            #
            # 1. remove duplicated urls (request_filter)
            # 2. check domain
            # 3. suffix filter
            # 4. allow static file with query or not
            # 5. extra filter
            #
            for url in self.__find_all_urls(response):
                logger.debug("checking: {}".format(url))
                # 1.
                method = str(request.method)
                if not self.request_filter.url_is_duplicate(url, method=method):
                    self.request_filter.add_url(url, method=method)
                    self.handlers.on_new_url(url)
                else:
                    continue

                # 2.
                domain, netloc = self.__extract_domainNnetloc(url)
                if domain not in self.domains:
                    self.handlers.on_new_domain(domain)
                    self.domains.add(domain)
                if domain in self.config.domain_blacklist:
                    continue
                if self.config.domain_whitelist:
                    if domain not in self.config.domain_whitelist:
                        continue
                else:
                    if self.config.allow_to_crawl_subdomain:
                        if netloc.endswith(self._init_netloc):
                            pass
                        else:
                            continue
                    else:
                        if netloc != self._init_netloc:
                            logger.debug("filtered by netloc: cur:{} != init:{}".format(netloc, self._init_netloc))
                            continue

                # 3.
                suffix, query = self.__extract_suffix(url)
                if suffix in self.config.suffix_blacklist:
                    # 4. config
                    if self.config.allow_static_file_with_query and query:
                        pass
                    else:
                        continue

                # 5. true for pass
                if not self.handlers.extra_url_checker(url):
                    continue

                defaul_request_params = self.config.default_request_params
                request = requests.Request(url=url, **defaul_request_params)
                self.worker_pool.execute(self.request, kwargs={
                    "request": request
                })

            # check whether the pipeline is finished.
            if self._finished >= self._started:
                self._have_started_event.clear()

        logger.info("dispatcher is finished.")

    def wait_until_finished(self):
        self.dispacher_thread.join()

    def __find_all_urls(self, response: requests.Response):
        """"""
        raw = response.text
        soup = bs4.BeautifulSoup(raw, 'html.parser')

        def genurls():
            for atag in soup.find_all():
                _url = atag.attrs.get("href")
                if _url:
                    yield self.__fix_url(_url)

                _url1 = atag.attrs.get("src")
                if _url1:
                    yield self.__fix_url(_url1)

        for _url in genurls():
            if _url:
                yield _url

    def __fix_url(self, url):
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
                return urljoin(self._init_request.url, url)

        url = basic()
        if not self.config.allow_fragment and url and "#" in url:
            return url[:url.index("#")]
        else:
            return url

    def __extract_domainNnetloc(self, url):
        nlc = urlparse(url).netloc
        domain = nlc[:nlc.index(":")] if ":" in nlc else nlc
        return domain, nlc

    def __extract_suffix(self, url):
        path = urlparse(url).path
        query = urlparse(url).query
        _, suffix = os.path.splitext(path)
        return suffix, query

    def _fake_execute(self, func, args=(), kwargs={}, id=None):
        self._old_execute(func, args, kwargs, id)
        self._started += 1

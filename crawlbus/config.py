#!/usr/bin/env python3
# coding:utf-8
import yaml

_DEFAULT_CONFIG = """
request_params:
  method: "GET"
  headers:
    User-Agent: "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
  data:
  params:
  auth:
  cookies:
  json:

crawler_options:
  fixed_cookie: false
  poolsize: 20
  callback_poolsize: 5
  url_simhash_distance: 2
  filter_dothtml: true
  ignore_param_value: false
  allow_fragment: false
  allow_to_crawl_subdomain: false
  allow_static_file_with_query: false
  
  domain_whitelist: []
  domain_blacklist: []
  
  suffix_blacklist: [
    ".pdf", ".zip",
    ".docx", ".doc", ".ppt", ".pptx",
    ".jpg", ".png", ".gif", ".jpeg"
    # ".js", ".css"
  ]
"""


class CrawlerConfig:

    def __init__(self, config={}):
        if not config:
            config = yaml.load(_DEFAULT_CONFIG)

        self._config = config
        self._options = config["crawler_options"]

    @property
    def default_request_params(self):
        """Adaptor for requests"""
        return dict(self._config["request_params"])

    @property
    def crawler_poolsize(self):
        """"""
        return int(self._config["crawler_options"]['poolsize'])

    poolsize = crawler_poolsize

    @property
    def callback_poolsize(self):
        """"""
        return int(self._config['crawler_options']['callback_poolsize'])

    @property
    def fixed_cookie(self):
        """"""
        return bool(self._options.get("fixed_cookie", False))

    @property
    def allow_to_crawl_subdomain(self):
        return bool(self._options.get("allow_to_crawl_subdomain", False))

    @property
    def url_simhash_distance(self):
        """"""
        return int(self._options["url_simhash_distance"])

    @property
    def filter_dothtml(self):
        """"""
        return bool(self._options['filter_dothtml'])

    @property
    def ignore_param_value(self):
        """"""
        return bool(self._options['ignore_param_value'])

    @property
    def allow_fragment(self):
        """"""
        return bool(self._options["allow_fragment"])

    @property
    def domain_whitelist(self):
        """"""
        return list(self._options['domain_whitelist'])

    @property
    def domain_blacklist(self):
        """"""
        return list(self._options['domain_blacklist'])

    @property
    def suffix_blacklist(self):
        """"""
        return list(self._options['suffix_blacklist'])

    @property
    def allow_static_file_with_query(self):
        """"""
        return bool(self._options['allow_static_file_with_query'])

    def merge_config_from_file(self, config_file):
        """"""
        with open(config_file) as fp:
            config = yaml.load(fp)

        return self._config.update(config)

    def merge_config_from_dict(self, dict_):
        self._config.update(dict_)


global_config = CrawlerConfig()



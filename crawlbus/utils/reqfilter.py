#!/usr/bin/env python3
import os
import simhash
import bloom_filter
import uuid
from urllib.parse import urlparse, urlunparse


class RequestFilter(object):
    def __init__(self, cap=100000, error_rate=0.0001):
        self.bfilter = bloom_filter.BloomFilter(
            max_elements=cap, error_rate=error_rate)

    def _concat_url(self, url, method="GET", **kwargs):
        kwargs['method'] = method
        items_str = "".join([f"{key}:{value}" for (key, value) in sorted(iter(kwargs.items()))])
        return f"{items_str}@{url}"

    def add_url(self, url, method="GET", **kwargs):
        _s = self._concat_url(url, method, **kwargs)
        self.bfilter.add(_s)

    def __in__(self, element):
        element = self._concat_url(url=element)
        return element in self.bfilter

    def is_duplicate(self, url, method="GET", **kwargs):
        return self._concat_url(url, method, **kwargs) in self.bfilter


STATIC_FILE_SUFFIX = [".html", ".htm", ".js", ".css"]


class FakeStaticPathFilter(object):
    def __init__(self, distance=2, bloomfilter=None, filter_dothtml=True, ignore_param_value=True):
        self.distance = distance
        self.filter_dothtml = filter_dothtml
        self.ignore_param_value = ignore_param_value
        self._simindex = simhash.SimhashIndex([], k=distance)
        self.bfilter = bloom_filter.BloomFilter() if not bloomfilter else bloom_filter

    def _prehandle_path(self, path):
        if "/" not in path:
            path = self._filter_staticfile(path)
            return f"/{path}"

        if not path.startswith("/"):
            path = f"/{path}"

        filename = self._filter_staticfile(os.path.basename(path))
        path = os.path.join(os.path.dirname(path), filename)

        last = path.split("/")[:-1]

        def generic(block: str):
            if block.isdigit() or block.replace("-", "").isdigit() or \
               block.replace(".", "").isdigit():
                return len(block) * "A"
            else:
                return block

        last = [generic(x) for x in last]
        return os.path.join("/".join(last), filename)

    def _filter_staticfile(self, filename):
        if self.filter_dothtml:
            if len([x for x in STATIC_FILE_SUFFIX if filename.endswith(x)]) > 0:
                return "S.STATIC"
            else:
                return filename
        else:
            return filename

    def _concat_url(self, url, path, **kwargs):
        parseresult = urlparse(url)
        if "&" in parseresult.query:
            allquery = sorted(iter(parseresult.query.split("&")))
            if not self.ignore_param_value:
                query = "&".join(allquery)
            else:
                _ret = []
                for e in allquery:
                    try:
                        _i = e.index('=')
                    except ValueError:
                        _i = None
                    _ret.append(e[:_i])
                query = "&".join(_ret)
        else:
            if not self.ignore_param_value:
                query = parseresult.query
            else:
                if "=" in parseresult.query:
                    query = parseresult.query.split("=")[0]
                else:
                    query = parseresult.query
        kwargs['query'] = query
        items_str = "".join([f"{key}:{value}" for (key, value) in sorted(iter(kwargs.items()))])
        # ParseResult(scheme, netloc, url, params, query, fragment)
        url = urlunparse([
            parseresult.scheme,
            parseresult.netloc,
            path,
            "",  # parseresult.params,
            "",  # parseresult.query,
            ""  # parseresult.fragment,
        ])
        return f"{items_str}@{url}"

    def add_url(self, url, **kwargs):
        _path = urlparse(url).path
        if not _path:
            _path = "/"
        _path = self._prehandle_path(_path)
        _final = self._concat_url(url, _path, **kwargs)

        if _final in self.bfilter:
            return
        else:
            self.bfilter.add(_final)

        if self.distance:
            _shash = simhash.Simhash(_final)
            result = self._simindex.get_near_dups(_shash)
            if not result:
                self._simindex.add(uuid.uuid4(), _shash)

    def url_is_duplicate(self, url, **kwargs):
        path = urlparse(url).path
        if not path:
            path = "/"
        _path = self._prehandle_path(path)
        _final = self._concat_url(url, _path, **kwargs)

        if _final not in self.bfilter:
            if not self.distance:
                return False
            else:
                shash = simhash.Simhash(_final)
                result = self._simindex.get_near_dups(shash)
                if not result:
                    return False
                else:
                    return True
        else:
            return True

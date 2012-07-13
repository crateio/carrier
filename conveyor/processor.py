from __future__ import absolute_import
from __future__ import division

import collections

from xmlrpc2 import client as xmlrpc2


class BaseProcessor(object):

    def __init__(self, index, *args, **kwargs):
        super(BaseProcessor, self).__init__(*args, **kwargs)

        self.index = index
        self.client = xmlrpc2.Client(self.index)

    def process(self):
        raise NotImplementedError

    def get_releases(self, name, version=None):
        if version is None:
            versions = self.client.package_releases(name, True)
        else:
            versions = [version]

        for version in versions:
            item = self.client.release_data(name, version)
            url = self.client.release_urls(item["name"], item["version"])

            if isinstance(url, collections.Mapping):
                urls = [url]
            elif isinstance(url, collections.Iterable):
                urls = url
            else:
                raise RuntimeError("Do not understand the type returned by release_urls")

            item.update({"files": urls})

            yield item


class BulkProcessor(BaseProcessor):

    def process(self):
        pass

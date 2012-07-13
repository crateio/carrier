from __future__ import absolute_import
from __future__ import division

import collections

import slumber
import slumber.exceptions
import xmlrpc2.client


class BaseProcessor(object):

    def __init__(self, index, warehouse, *args, **kwargs):
        super(BaseProcessor, self).__init__(*args, **kwargs)

        wargs, wkwargs = warehouse

        self.client = xmlrpc2.client.Client(index)
        self.warehouse = slumber.API(*wargs, **wkwargs)

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

    def sync_release(self, release):
        # Get or Create Project
        try:
            project = self.warehouse.projects(release["name"]).get()
        except slumber.exceptions.HttpClientError as e:
            if e.response.status_code == 404:
                project = self.warehouse.projects.post({"name": release["name"]})["resource_uri"]

        # @@@ Create Version

        # @@@ Create File


class BulkProcessor(BaseProcessor):

    def process(self):
        pass

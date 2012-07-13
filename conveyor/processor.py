from __future__ import absolute_import
from __future__ import division

import collections
import re

import slumber
import slumber.exceptions
import xmlrpc2.client


_normalize_regex = re.compile(r"[^A-Za-z0-9.]+")


def get(d, attr, default=None):
    value = d.get(attr, default)

    if not value:
        value = default

    return value


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

            item.update({
                "normalized": _normalize_regex.sub("-", item["name"]).lower(),
                "files": urls,
            })

            yield item

    def sync_release(self, release):
        # Get or Create Project
        # @@@ Update project if it already existed
        try:
            project = self.warehouse.projects(release["normalized"]).get()
        except slumber.exceptions.HttpClientError as e:
            if e.response.status_code == 404:
                data = self.to_warehouse_project(release)
                project = self.warehouse.projects.post(data)

        # Get or Create Version
        # @@@ Update Version if it already existed
        try:
            version = self.warehouse.projects(release["normalized"]).versions(release["version"]).get()
        except slumber.exceptions.HttpClientError as e:
            if e.response.status_code == 404:
                data = self.to_warehouse_version(release, extra={"project": project})
                version = self.warehouse.projects(release["normalized"]).versions().post(data)

        # @@@ Get or Create Files

    def to_warehouse_project(self, release, extra=None):
        data = {"name": release["name"]}

        if extra is not None:
            data.update(extra)

        return data

    def to_warehouse_version(self, release, extra=None):
        pass

    def to_warehouse_file(self, release, extra=None):
        pass


class BulkProcessor(BaseProcessor):

    def process(self):
        pass

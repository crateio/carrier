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

    if not value or value == "UNKNOWN":
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

            if isinstance(versions, basestring):
                versions = [versions]
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
        try:
            project = self.warehouse.projects(release["normalized"]).get()
        except slumber.exceptions.HttpClientError as e:
            if not e.response.status_code == 404:
                raise

            data = self.to_warehouse_project(release)
            project = self.warehouse.projects.post(data)
        else:
            # @@@ Update project
            pass

        # Get or Create Version
        try:
            version = self.warehouse.projects(release["normalized"]).versions(release["version"]).get()
        except slumber.exceptions.HttpClientError as e:
            if not e.response.status_code == 404:
                raise

            data = self.to_warehouse_version(release, extra={"project": project["resource_uri"]})
            version = self.warehouse.projects(release["normalized"]).versions().post(data)
        else:
            # @@@ Update Version
            pass

        # @@@ Get/Create/Delete Files
        # @@@ Batch Update?

    def to_warehouse_project(self, release, extra=None):
        data = {"name": release["name"]}

        if extra is not None:
            data.update(extra)

        return data

    def to_warehouse_version(self, release, extra=None):
        data = {
            "version": release["version"],

            "summary": get(release, "summary", ""),
            "description": get(release, "description", ""),
            "license": get(release, "license", ""),

            "author": {
                "name": get(release, "author", ""),
                "email": get(release, "author_email", ""),
            },
            "maintainer": {
                "name": get(release, "maintainer", ""),
                "email": get(release, "maintainer_email", ""),
            },

            "classifiers": get(release, "classifiers"),
            "uris": {},

            "requires_python": get(release, "requires_python", ""),
        }

        if get(release, "download_url"):
            data["uris"]["Download"] = release["download_url"]

        if get(release, "home_page"):
            data["uris"]["Home page"] = release["home_page"]

        if get(release, "bugtrack_url"):
            data["uris"]["Bug tracker"] = release["bugtrack_url"]

        if get(release, "docs_url"):
            data["uris"]["Documentation"] = release["docs_url"]

        if get(release, "platform"):
            platforms = get(release, "platform")

            if isinstance(platforms, basestring):
                platforms = [platforms]

            data["platforms"] = platforms

        if get(release, "keywords"):
            keywords = get(release, "keywords")

            # Check for a comma
            if "," in keywords:
                keywords = [x.strip() for x in keywords.split(",")]
            else:
                keywords = [x.strip() for x in keywords.split()]

            data["keywords"] = keywords

        if extra is not None:
            data.update(extra)

        return data

    def to_warehouse_file(self, release, extra=None):
        pass


class BulkProcessor(BaseProcessor):

    def process(self):
        pass

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import base64
import collections
import hashlib

import requests

from .exceptions import HashMismatch


class Package(object):

    unneeded_fields = set([
                        "_pypi_hidden",
                        "_pypi_ordering",
                        "cheesecake_code_kwalitee_id",
                        "cheesecake_documentation_id",
                        "cheesecake_installability_id"
                    ])

    def __init__(self, client, package, version=None, *args, **kwargs):
        super(Package, self).__init__(*args, **kwargs)

        self.client = client
        self.package = package
        self.version = version

        self.session = requests.session()

    def versions(self):
        if self.version is None:
            versions = self.client.package_releases(self.package, True)

            if isinstance(versions, basestring):
                versions = [versions]
        else:
            versions = [self.version]

        return versions

    def releases(self):
        for version in self.versions():
            item = self.client.release_data(self.package, version)

            if not item:
                continue

            # fix classifiers
            item["classifiers"] = sorted(set(item.get("classifiers", [])))

            # Include the files
            item["files"] = self.files(version)

            # Remove unneeded fields
            for field in self.unneeded_fields:
                del item[field]

            yield item

    def files(self, version):
        urls = self.client.release_urls(self.package, version)

        if isinstance(urls, collections.Mapping):
            urls = [urls]
        elif isinstance(urls, collections.Iterable):
            pass  # No action is required if it's already iterable
        else:
            raise ValueError("Do not understand the type returned by release_urls")

        files = []

        for url in urls:
            resp = self.session.get(url["url"], prefetch=True)
            resp.raise_for_status()

            file_hash = hashlib.md5(resp.content)

            if url["md5_digest"] != file_hash.hexdigest():
                raise HashMismatch("'MD5 hash {hash}' does not match the expected '{expected}' for {url}".format(hash=file_hash.hexdigest(), expected=url["md5_digest"], url=url["url"]))

            url["file"] = base64.b64encode(resp.content)

            # Remove this field because it'll cause issues with consistent hashing of this resource
            del url["downloads"]

            files.append(url)

        return files

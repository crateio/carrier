from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import base64
import collections
import hashlib
import json

import requests

from .exceptions import HashMismatch
from .utils import NormalizingDict, clean_uri, split_meta


class File(object):

    def __init__(self, *args, **kwargs):
        kwargs = NormalizingDict(kwargs.items())

        # Useful data
        self.comment = kwargs.pop("comment_text")
        self.filename = kwargs.pop("filename")
        self.type = kwargs.pop("packagetype")
        self.python_version = kwargs.pop("python_version")
        self.created = kwargs.pop("upload_time")
        self.data = kwargs.pop("file")

        # PyPI internal data
        self._downloads = kwargs.pop("downloads")
        self._has_sig = kwargs.pop("has_sig")
        self._md5_digest = kwargs.pop("md5_digest")
        self._size = kwargs.pop("size")
        self._url = kwargs.pop("url")

        super(File, self).__init__(*args, **kwargs)

    def serialize(self):
        data = {
            "file": {
                "name": self.filename,
                "file": self.data,
            },
            "created": self.created,
            "type": self.type,
            "python_version": self.python_version,
            "comment": self.comment,
            "filename": self.filename,
            "filesize": self._size,
            "digests": {
                "md5": hashlib.md5(base64.b64decode(self.data)).hexdigest(),
                "sha256": hashlib.sha256(base64.b64decode(self.data)).hexdigest(),
            },
        }

        return data


class Release(object):

    def __init__(self, *args, **kwargs):
        kwargs = NormalizingDict(kwargs.items())

        # Useful data
        self.author = kwargs.pop("author", None)
        self.author_email = kwargs.pop("author_email", None)

        self.classifiers = kwargs.pop("classifiers", [])

        self.description = kwargs.pop("description", None)

        self.keywords = kwargs.pop("keywords", "")

        # Check for a comma
        if "," in self.keywords:
            self.keywords = [x.strip() for x in self.keywords.split(",")]
        else:
            self.keywords = [x.strip() for x in self.keywords.split()]

        self.license = kwargs.pop("license", None)

        self.maintainer = kwargs.pop("maintainer", None)
        self.maintainer_email = kwargs.pop("maintainer_email", None)

        self.name = kwargs.pop("name", None)

        self.platforms = kwargs.pop("platform", [])

        if isinstance(self.platforms, basestring):
            self.platforms = [self.platforms]

        self.supported_platforms = kwargs.pop("supported_platforms", [])

        if isinstance(self.supported_platforms, basestring):
            self.supported_platforms = [self.supported_platforms]

        self.requires_python = kwargs.pop("requires_python", None)

        self.summary = kwargs.pop("summary", None)

        self.version = kwargs.pop("version", None)

        self.uris = {}

        for key, label in {"bugtrack_url": "Bug tracker", "home_page": "Home page", "download_url": "Download", "docs_url": "Documentation"}.items():
            uri = kwargs.pop(key, None)

            if uri is not None:
                try:
                    self.uris[label] = clean_uri(uri)
                except ValueError:
                    pass

        for purl in kwargs.pop("project_url", []):
            label, uri = purl.split(",", 1)

            try:
                self.uris[label] = clean_uri(uri)
            except ValueError:
                pass

        self.requires = [split_meta(req) for req in kwargs.pop("requires_dist", [])]
        self.provides = [split_meta(req) for req in kwargs.pop("provides_dist", [])]
        self.obsoletes = [split_meta(req) for req in kwargs.pop("obsoletes_dist", [])]

        self.requires_external = kwargs.pop("requires_external", [])

        self._files = [File(**x) for x in kwargs.pop("files", [])]

        # Old and useless
        self._old_requires = kwargs.pop("requires", [])
        self._old_provides = kwargs.pop("provides", [])
        self._old_obsoletes = kwargs.pop("obsoletes", [])

        # PyPI internal data
        self._package_url = kwargs.pop("package_url", None)
        self._release_url = kwargs.pop("release_url", None)

        self._cheesecake_code_kwalitee_id = kwargs.pop("cheesecake_code_kwalitee_id", None)
        self._cheesecake_documentation_id = kwargs.pop("cheesecake_documentation_id", None)
        self._cheesecake_installability_id = kwargs.pop("cheesecake_installability_id", None)

        self._pypi_hidden = kwargs.pop("_pypi_hidden", None)
        self._pypi_ordering = kwargs.pop("_pypi_ordering", None)

        self._stable_version = kwargs.pop("stable_version", None)

        super(Release, self).__init__(*args, **kwargs)

    @property
    def files(self):
        return self._files

    def serialize(self):
        data = {}

        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            data[key] = value

        return data

    def hash(self):
        def _dict_constant_data_structure(dictionary):
            data = []

            for k, v in dictionary.items():
                if isinstance(v, dict):
                    v = _dict_constant_data_structure(v)
                elif isinstance(v, set):
                    v = sorted(v)
                data.append([k, v])

            return sorted(data, key=lambda x: x[0])

        data = self.serialize()
        data["files"] = [f.serialize() for f in self.files]
        data = json.dumps(_dict_constant_data_structure(data), default=lambda obj: obj.isoformat() if hasattr(obj, "isoformat") else obj)

        return hashlib.sha512(data).hexdigest()[:32]

    def changed(self, other):
        return not self.hash() == other


class Package(object):

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

            yield Release(**item)

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

            files.append(url)

        return files

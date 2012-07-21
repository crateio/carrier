from __future__ import absolute_import
from __future__ import division

import base64
import collections
import datetime
import hashlib
import logging
import json
import re
import time

import pytz
import requests
import slumber.exceptions
import xmlrpc2.client

from conveyor.utils import DictDiffer


_normalize_regex = re.compile(r"[^A-Za-z0-9.]+")
_distutils2_version_capture = re.compile("^(.*?)(?:\(([^()]+)\))?$")


logger = logging.getLogger(__name__)


EXPECTED = set(["resource_uri", "yanked", "downloads", "modified"])


def split_meta(meta):
    meta_split = meta.split(";", 1)
    meta_name, meta_version = _distutils2_version_capture.search(meta_split[0].strip()).groups()
    meta_env = meta_split[1].strip() if len(meta_split) == 2 else ""

    return {
        "name": meta_name,
        "version": meta_version if meta_version is not None else "",
        "environment": meta_env,
    }


def get(d, attr, default=None):
    value = d.get(attr, default)

    if not value or value in ["UNKNOWN", "None"]:
        value = default

    return value


def get_key(prefix, key):
    if not prefix is None:
        return "%s:%s" % (prefix, key)
    return key


class BaseProcessor(object):

    def __init__(self, index, warehouse, session=None, store=None, store_prefix=None, *args, **kwargs):
        super(BaseProcessor, self).__init__(*args, **kwargs)

        if session is None:
            session = requests.session()

        self.session = session

        self.client = xmlrpc2.client.Client(index, session=self.session)
        self.warehouse = warehouse
        self.store = store
        self.store_prefix = store_prefix

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

            files = []

            oldest = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

            for url in urls:
                data = url.copy()

                try:
                    resp = self.session.get(data["url"], prefetch=True)
                    resp.raise_for_status()
                except Exception:
                    # @@@ Catch the proper exceptions (and do what?)
                    raise

                if oldest > data["upload_time"]:
                    oldest = data["upload_time"]

                data["file_data"] = base64.b64encode(resp.content)
                files.append(data)

            item.update({
                "normalized": _normalize_regex.sub("-", item["name"]).lower(),
                "files": files,
            })

            if files:
                item["guessed_creation"] = oldest

            yield item

    def sync_release(self, release):
        if "/" in release["version"]:
            # We cannot accept versions with a / in it.
            logger.error("Skipping '%s' version '%s' because it contains a '/'", release["name"], release["version"])
            return

        key = get_key(self.store_prefix, "pypi:process:%s:%s" % (release["name"], release["version"]))

        stored_hash = self.store.get(key)
        computed_hash = hashlib.sha224(json.dumps(release, default=lambda obj: obj.isoformat() if hasattr(obj, "isoformat") else obj)).hexdigest()

        if stored_hash and stored_hash == computed_hash:
            logger.info("Skipping '%s' version '%s' because it has not changed", release["name"], release["version"])
            return

        logger.info("Syncing '%s' version '%s'", release["name"], release["version"])

        # Get or Create Project
        try:
            # Get
            project = self.warehouse.projects(release["normalized"]).get()
        except slumber.exceptions.HttpClientError as e:
            if not e.response.status_code == 404:
                raise

            # Create
            project_data = self.to_warehouse_project(release)
            project = self.warehouse.projects.post(project_data)

        # Get or Create Version
        version_data = self.to_warehouse_version(release, extra={"project": project["resource_uri"]})

        try:
            # GET
            version = self.warehouse.projects(release["normalized"]).versions(release["version"]).get()
        except slumber.exceptions.HttpClientError as e:
            if not e.response.status_code == 404:
                raise

            # Create
            version = self.warehouse.projects(release["normalized"]).versions().post(version_data)
        else:
            # Update
            diff = DictDiffer(version_data, version)
            different = diff.added() | diff.changed() | diff.removed() - (EXPECTED | set(["files"]))

            if "created" in different and not version.get("files", None):
                # The created time is a guess because we don't have any uploaded files
                different.remove("created")

            if different:
                logger.info(
                    "Updating the version for '%s' version '%s'. warehouse: '%s' updated: '%s'",
                    release["name"],
                    release["version"],
                    dict([(k, v) for k, v in version.items() if k in different]),
                    dict([(k, v) for k, v in version_data.items() if k in different]),
                )
                self.warehouse.projects(release["normalized"]).versions(release["version"]).put(version_data)
                version = self.warehouse.projects(release["normalized"]).versions(release["version"]).get()

        for f in release["files"]:
            file_data = self.to_warehouse_file(release, f, extra={"version": version["resource_uri"]})

            try:
                # Get
                vfile = self.warehouse.projects(release["normalized"]).versions(release["version"]).files(f["filename"]).get()
            except slumber.exceptions.HttpClientError as e:
                if not e.response.status_code == 404:
                    raise

                # Create
                vfile = self.warehouse.projects(release["normalized"]).versions(release["version"]).files.post(file_data)
            else:
                # Update
                diff = DictDiffer(file_data, vfile)
                different = diff.added() | (diff.changed() - set(["file"])) | (diff.removed() - EXPECTED)

                if different:
                    logger.info(
                        "Updating the file '%s' for '%s' version '%s'. warehouse: '%s' updated: '%s'",
                        f["filename"],
                        release["name"],
                        release["version"],
                        dict([(k, v) for k, v in vfile.items() if k in different]),
                        dict([(k, v) for k, v in file_data.items() if k in different]),
                    )
                    self.warehouse.projects(release["normalized"]).versions(release["version"]).files(f["filename"]).put(file_data)
                    vfile = self.warehouse.projects(release["normalized"]).versions(release["version"]).files(f["filename"]).get()

        self.store.setex(key, 604800, computed_hash)

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

            "author": {},
            "maintainer": {},

            "classifiers": get(release, "classifiers", []),
            "uris": {},

            "requires_python": get(release, "requires_python", ""),
            "requires_external": get(release, "requires_external", []),

            "platforms": [],
            "supported_platforms": [],
            "keywords": [],
        }

        if get(release, "author", None):
            data["author"]["name"] = release["author"]

        if get(release, "author_email", None):
            data["author"]["email"] = release["author_email"]

        if get(release, "maintainer", None):
            data["maintainer"]["name"] = release["maintainer"]

        if get(release, "maintainer_email", None):
            data["maintainer"]["email"] = release["maintainer_email"]

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

        if get(release, "supported_platforms"):
            supported_platforms = get(release, "supported_platforms")

            if isinstance(supported_platforms, basestring):
                supported_platforms = [supported_platforms]

            data["supported_platforms"] = supported_platforms

        if get(release, "keywords"):
            keywords = get(release, "keywords")

            # Check for a comma
            if "," in keywords:
                keywords = [x.strip() for x in keywords.split(",")]
            else:
                keywords = [x.strip() for x in keywords.split()]

            data["keywords"] = keywords

        for url in get(release, "project_url", []):
            label, uri = url.split(",", 1)
            data["uris"][label] = uri

        data["requires"] = [split_meta(req) for req in get(release, "requires_dist", [])]
        data["provides"] = [split_meta(req) for req in get(release, "provides_dist", [])]
        data["obsoletes"] = [split_meta(req) for req in get(release, "obsoletes_dist", [])]

        if get(release, "guessed_creation", None) is not None:
            data["created"] = release["guessed_creation"].isoformat()

        if extra is not None:
            data.update(extra)

        return data

    def to_warehouse_file(self, release, file, extra=None):
        data = {
            "file": {"name": file["filename"], "file": file["file_data"]},
            "created": file["upload_time"].isoformat(),
            "type": file["packagetype"],
            "python_version": file["python_version"],
            "comment": file["comment_text"],
        }

        raw_data = base64.b64decode(file["file_data"])

        data.update({
            "digests": {
                "md5": hashlib.md5(raw_data).hexdigest(),
                "sha256": hashlib.sha256(raw_data).hexdigest(),
            },
            "filesize": len(raw_data),
            "filename": file["filename"],
        })

        if extra is not None:
            data.update(extra)

        return data


class BulkProcessor(BaseProcessor):

    def process(self):
        # @@@ Should we handle attempting to delete?

        current = time.mktime(datetime.datetime.utcnow().timetuple())

        names = set(self.client.list_packages())

        for package in names:
            for release in self.get_releases(package):
                self.sync_release(release)

        self.store.set(get_key(self.store_prefix, "pypi:since"), current)

from __future__ import absolute_import
from __future__ import division

import base64
import collections
import datetime
import hashlib
import json
import re
import time

import pytz
import requests
import slumber
import slumber.exceptions
import xmlrpc2.client

from conveyor.store import InMemoryStore


_normalize_regex = re.compile(r"[^A-Za-z0-9.]+")
_distutils2_version_capture = re.compile("^(.*?)(?:\(([^()]+)\))?$")


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


class BaseProcessor(object):

    def __init__(self, index, warehouse, session=None, store=None, *args, **kwargs):
        super(BaseProcessor, self).__init__(*args, **kwargs)

        wargs, wkwargs = warehouse

        if session is None:
            session = requests.session()

        self.session = session

        self.client = xmlrpc2.client.Client(index, session=self.session)
        self.warehouse = slumber.API(*wargs, **wkwargs)

        if store is None:
            self.store = InMemoryStore()
        else:
            self.store = store

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
                "guessed_creation": oldest,
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

        for f in release["files"]:
            try:
                vfile = self.warehouse.projects(release["normalized"]).versions(release["version"]).files(f["filename"]).get()
            except slumber.exceptions.HttpClientError as e:
                if not e.response.status_code == 404:
                    raise
                try:
                    data = self.to_warehouse_file(release, f, extra={"version": version["resource_uri"]})
                    vfile = self.warehouse.projects(release["normalized"]).versions(release["version"]).files.post(data)
                except Exception as e:
                    print e.response.content
                    raise
            else:
                # @@@ Update File
                pass

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
            "requires_external": get(release, "requires_external", []),
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
                stored_hash = self.store.get("pypi:process:%s:%s" % (release["name"], release["version"]))
                computed_hash = hashlib.sha224(json.dumps(release, default=lambda obj: obj.isoformat() if hasattr(obj, "isoformat") else obj)).hexdigest()

                if not stored_hash or stored_hash != computed_hash:
                    print "Syncing", release["name"], release["version"]
                    self.sync_release(release)
                    self.store.setex("pypi:process:%s:%s" % (release["name"], release["version"]), computed_hash, 604800)
                else:
                    print "Skipping", release["name"], release["version"]

        self.store.set("pypi:since", current)

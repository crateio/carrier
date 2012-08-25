from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import base64
import collections
import datetime
import hashlib
import logging
import json
import re
import time

import requests
import xmlrpc2.client

from .pypi import Package
from .utils import clean_url


_normalize_regex = re.compile(r"[^A-Za-z0-9.]+")
_distutils2_version_capture = re.compile("^(.*?)(?:\(([^()]+)\))?$")


logger = logging.getLogger(__name__)


EXPECTED = set(["resource_uri", "downloads", "modified"])


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


class Processor(object):

    def __init__(self, index, warehouse, session=None, store=None, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

        if session is None:
            session = requests.session()

        self.session = session

        self.client = xmlrpc2.client.Client(index, session=self.session)
        self.warehouse = warehouse
        self.store = store

    def compute_hash(self, release):
        def _dict_constant_data_structure(dictionary):
            data = []

            for k, v in dictionary.items():
                if isinstance(v, dict):
                    v = _dict_constant_data_structure(v)
                elif isinstance(v, set):
                    v = sorted(v)
                data.append([k, v])

            return sorted(data, key=lambda x: x[0])

        if not hasattr(self, "_computed_hash"):
            self._computed_hash = hashlib.sha512(json.dumps(
                                                    _dict_constant_data_structure(release),
                                                    default=lambda obj: obj.isoformat() if hasattr(obj, "isoformat") else obj)
                                                ).hexdigest()[:32]

        return self._computed_hash

    def release_changed(self, release):
        key = "pypi:process:%s:%s" % (release["name"], release["version"])

        stored_hash = self.store.get(key)
        computed_hash = self.compute_hash(release)

        return not (stored_hash and stored_hash == computed_hash)

    def store_release_hash(self, release):
        key = "pypi:process:%s:%s" % (release["name"], release["version"])
        self.store.set(key, self.compute_hash(release))

    def get_and_update_or_create_version(self, release, project):
        version_data = self.to_warehouse_version(release, extra={"project": project})

        version, created = self.warehouse.versions.objects.get_or_create(project__name=project.name, version=release["version"], show_yanked=True, defaults=version_data)

        if not created:
            version.classifiers = sorted(version.classifiers)

            changed = False

            for k, v in version_data.iteritems():
                if getattr(version, k, None) != v:
                    changed = True
                    setattr(version, k, v)

            if changed:
                version.save()

        return version

    def get_and_update_or_create_file(self, release, version, distribution):
        file_data = self.to_warehouse_file(release, distribution, extra={"version": version})

        vfile, created = self.warehouse.files.objects.get_or_create(filename=file_data["filename"], show_yanked=True, defaults=file_data)

        if not created:
            changed = False

            for k, v in file_data.iteritems():
                if getattr(vfile, k, None) != v:
                    changed = True
                    setattr(vfile, k, v)

            if changed:
                vfile.save()

        return vfile

    def sync_files(self, release, version):
        # Determine if any files need to be deleted
        warehouse_files = set([f.filename for f in version.files])
        local_files = set([x["filename"] for x in release["files"]])
        deleted = warehouse_files - local_files

        # Delete any files that need to be deleted
        if deleted:
            for filename in deleted:
                logger.info("Deleting the file '%s' from '%s' version '%s'", filename, release["name"], release["version"])

            self.warehouse.files.objects.filter(filename__in=deleted).delete()

        return [self.get_and_update_or_create_file(release, version, distribution) for distribution in release["files"]]

    def sync_release(self, release):
        if "/" in release["version"]:
            # We cannot accept versions with a / in it.
            logger.error("Skipping '%s' version '%s' because it contains a '/'", release["name"], release["version"])
            return

        if not self.release_changed(release):
            logger.info("Skipping '%s' version '%s' because it has not changed", release["name"], release["version"])
            return

        logger.info("Syncing '%s' version '%s'", release["name"], release["version"])

        project, _ = self.warehouse.projects.objects.get_or_create(name=release["name"])
        version = self.get_and_update_or_create_version(release, project)
        files = self.sync_files(release, version)

        self.store_release_hash(release)

        return {
            "project": project,
            "version": version,
            "files": files,
        }

    def to_warehouse_version(self, release, extra=None):
        data = {
            "version": release["version"],

            "summary": get(release, "summary", ""),
            "description": get(release, "description", ""),
            "license": get(release, "license", ""),

            "author": get(release, "author", ""),
            "author_email": get(release, "author_email", ""),

            "maintainer":  get(release, "maintainer", ""),
            "maintainer_email": get(release, "maintainer_email", ""),

            "classifiers": get(release, "classifiers", []),
            "uris": {},

            "requires_python": get(release, "requires_python", ""),
            "requires_external": get(release, "requires_external", []),

            "platforms": [],
            "supported_platforms": [],
            "keywords": [],

            "yanked": False,
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

        # Clean the URI fields
        cleaned_uris = {}
        for k, v in data["uris"].iteritems():
            try:
                cleaned_uris[k] = clean_url(v)
            except ValueError:
                pass
        data["uris"] = cleaned_uris

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

            "yanked": False,
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

    def delete_project_version(self, package, version):
        key = "pypi:process:%s:%s" % (package, version)

        logger.info("Deleting version '%s' of '%s'", version, package)

        self.store.delete(key)
        self.warehouse.versions.objects.filter(project__name=package, version=version).delete()

    def delete_project(self, project):
        search_key = "pypi:process:%s:*" % project

        logger.info("Deleting '%s'", project)

        for k in self.store.keys(search_key):
            self.store.delete(k)

        self.warehouse.projects.objects.filter(name=project).delete()

    def update(self, name, version, timestamp, action, matches):
        package = Package(self.client, name, version)

        for release in package.releases():
            self.sync_release(release)

    def delete(self, name, version, timestamp, action, matches):
        filename = None

        if action == "remove":
            if version is None:
                obj = self.warehouse.projects.objects.filter(name=name).get()
                logger.info("Deleting '%s'", name)
            else:
                obj = self.warehouse.versions.objects.filter(project__name=name, version=version).get()
                logger.info("Deleting '%s' version '%s'", name, version)
        elif action.startswith("remove file"):
            filename = matches.groups()[0]
            obj = self.warehouse.files.objects.get(filename=filename)
            logger.info("Deleting '%s' version '%s' filename '%s'", name, version, filename)
        else:
            raise RuntimeError("Unknown Action passed to delete()")

        if version is None:
            key_pattern = "pypi:process:%s:*" % name
            keys = self.store.keys(key_pattern)
        else:
            keys = ["pypi:process:%s:%s" % (name, version)]

        for key in keys:
            self.store.delete(key)

        obj.delete()

    def process(self):
        logger.info("Starting changed projects synchronization")

        current = time.mktime(datetime.datetime.utcnow().timetuple())

        since = int(float(self.store.get("pypi:since"))) - 10

        dispatch = collections.OrderedDict([
            (re.compile("^create$"), self.update),
            (re.compile("^new release$"), self.update),
            (re.compile("^add [\w\d\.]+ file .+$"), self.update),
            (re.compile("^remove$"), self.delete),
            (re.compile("^remove file (.+)$"), self.delete),
            (re.compile("^update [\w]+(, [\w]+)*$"), self.update),
            #(re.compile("^docupdate$"), docupdate),  # @@@ Do Something
            #(re.compile("^add (Owner|Maintainer) .+$"), add_user_role),  # @@@ Do Something
            #(re.compile("^remove (Owner|Maintainer) .+$"), remove_user_role),  # @@@ Do Something
        ])

        changes = self.client.changelog(since)

        if changes:
            if isinstance(changes[0], basestring):
                changes = [changes]

        for name, version, timestamp, action in changes:
            action_hash = hashlib.sha512(":".join([str(x) for x in [name, version, timestamp, action]])).hexdigest()[:32]
            action_key = "pypi:changelog:%s" % action_hash

            logdata = {"action": action, "name": name, "version": version, "timestamp": timestamp}

            if not self.store.exists(action_key):
                logger.debug("Processing %(name)s %(version)s %(timestamp)s %(action)s" % logdata)

                # Dispatch Based on the action
                for pattern, func in dispatch.iteritems():
                    matches = pattern.search(action)
                    if matches is not None:
                        func(name, version, timestamp, action, matches)
                        break

                self.store.setex(action_key, 2592000, "1")
            else:
                logger.debug("Skipping %(name)s %(version)s %(timestamp)s %(action)s" % logdata)

        self.store.set("pypi:since", current)

        logger.info("Finished changed projects synchronization")

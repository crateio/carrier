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

from conveyor.utils import DictDiffer, clean_url


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


def get_key(prefix, key):
    if not prefix is None:
        return "%s:%s" % (prefix, key)
    return key


class Processor(object):

    def __init__(self, index, warehouse, session=None, store=None, store_prefix=None, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

        if session is None:
            session = requests.session()

        self.session = session

        self.client = xmlrpc2.client.Client(index, session=self.session)
        self.warehouse = warehouse
        self.store = store
        self.store_prefix = store_prefix

    def get_releases(self, name, version=None):
        if version is None:
            versions = self.client.package_releases(name, True)

            if isinstance(versions, basestring):
                versions = [versions]
        else:
            versions = [version]

        for version in versions:
            item = self.client.release_data(name, version)

            if not item:
                continue

            url = self.client.release_urls(item["name"], item["version"])

            if isinstance(url, collections.Mapping):
                urls = [url]
            elif isinstance(url, collections.Iterable):
                urls = url
            else:
                raise RuntimeError("Do not understand the type returned by release_urls")

            # fix classifiers
            item["classifiers"] = sorted(set(get(item, "classifiers", [])))

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

            # Clean up some of the fields that we don't use
            remove_fields = set([
                "_pypi_hidden",
                "_pypi_ordering",
                "cheesecake_code_kwalitee_id",
                "cheesecake_documentation_id",
                "cheesecake_installability_id",
            ])

            item = dict([(k, v) for k, v in item.items() if k not in remove_fields])

            yield item

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
        key = get_key(self.store_prefix, "pypi:process:%s:%s" % (release["name"], release["version"]))

        stored_hash = self.store.get(key)
        computed_hash = self.compute_hash(release)

        return not (stored_hash and stored_hash == computed_hash)

    def store_release_hash(self, release):
        key = get_key(self.store_prefix, "pypi:process:%s:%s" % (release["name"], release["version"]))
        self.store.set(key, self.compute_hash(release))

    def get_and_update_or_create_version(self, release, project):
        version_data = self.to_warehouse_version(release)

        version, c = self.warehouse.versions.objects.get_or_create(project=project, version=release["version"], defaults=version_data)

        if not c:
            # Update
            version["classifiers"] = sorted(version["classifiers"])
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

                for k, v in version_data.iteritems():
                    setattr(version, k, v)

                version.save()

        return version

    def get_and_update_or_create_file(self, release, version, distribution):
        file_data = self.to_warehouse_file(release, distribution, extra={"version": version})

        vfile, c = self.warehouse.files.objects.get_or_create(filename=file_data["filename"], defaults=file_data)

        if not c:
            # Update
            diff = DictDiffer(file_data, vfile)
            different = diff.added() | (diff.changed() - set(["file"])) | (diff.removed() - EXPECTED)

            if different:
                logger.info(
                    "Updating the file '%s' for '%s' version '%s'. warehouse: '%s' updated: '%s'",
                    distribution["filename"],
                    release["name"],
                    release["version"],
                    dict([(k, v) for k, v in vfile.items() if k in different]),
                    dict([(k, v) for k, v in file_data.items() if k in different]),
                )

                for k, v in file_data.iteritems():
                    setattr(vfile, k, v)

                vfile.save()

        return vfile

    def sync_files(self, release, version):
        # Determine if any files need to be deleted
        warehouse_files = set([f.filename for f in version.files])
        local_files = set([x["filename"] for x in release["files"]])
        deleted = warehouse_files - local_files

        # Delete any files that need to be deleted
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

    def get_warehouse_releases(self, package):
        # @@@ Implement paging

        try:
            versions = self.warehouse.versions.get(project__name=package, limit=1000)
        except slumber.exceptions.HttpClientError as e:
            if not e.response.status_code == 404:
                logger.error(e.response.content)
                raise

            return set()

        return set([v["version"] for v in versions["objects"]])

    def delete_project_version(self, package, version):
        normalized = _normalize_regex.sub("-", package).lower()
        key = get_key(self.store_prefix, "pypi:process:%s:%s" % (package, version))

        logger.info("Deleting version '%s' of '%s'", version, package)

        self.store.delete(key)
        self.warehouse.versions("/".join([normalized, version])).delete()

    def delete_project(self, project):
        normalized = _normalize_regex.sub("-", project).lower()
        search_key = get_key(self.store_prefix, "pypi:process:%s:*" % project)

        logger.info("Deleting '%s'", project)

        for k in self.store.keys(search_key):
            self.store.delete(k)

        self.warehouse.projects(normalized).delete()

    def update(self, name, version, timestamp, action, matches):
        for release in self.get_releases(name, version=version):
            self.sync_release(release)

    def delete(self, name, version, timestamp, action, matches):
        normalized = _normalize_regex.sub("-", name).lower()
        filename = None

        if action == "remove":
            if version is None:
                obj = self.warehouse.projects(normalized)
                logger.info("Deleting '%s'", name)
            else:
                obj = self.warehouse.versions("/".join([normalized, version]))
                logger.info("Deleting '%s' version '%s'", name, version)
        elif action.startswith("remove file"):
            filename = matches.groups()[0]
            obj = self.warehouse.files(filename)
            logger.info("Deleting '%s' version '%s' filename '%s'", name, version, filename)
        else:
            raise RuntimeError("Unknown Action passed to delete()")

        if version is None:
            key_pattern = get_key(self.store_prefix, "pypi:process:%s:*" % name)
            keys = self.store.keys(key_pattern)
        else:
            keys = [get_key(self.store_prefix, "pypi:process:%s:%s" % (name, version))]

        for key in keys:
            self.store.delete(key)

        try:
            obj.delete()
        except slumber.exceptions.HttpClientError as e:
            if not e.response.status_code == 404:
                logger.error(e.response.content)
                raise
            msg = "404 received trying to delete %s" % name

            if version is not None:
                msg += " version '%s'" % version

            if filename is not None:
                msg += "filename '%s'" % filename

            logger.warning(msg)

    def process(self):
        # @@@ Handle Deletion

        logger.info("Starting changed projects synchronization")

        current = time.mktime(datetime.datetime.utcnow().timetuple())

        since = int(float(self.store.get(get_key(self.store_prefix, "pypi:since")))) - 10

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
            action_key = get_key(self.store_prefix, "pypi:changelog:%s" % action_hash)

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

        self.store.set(get_key(self.store_prefix, "pypi:since"), current)

        logger.info("Finished changed projects synchronization")

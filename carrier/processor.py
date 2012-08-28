from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections
import datetime
import hashlib
import logging
import re
import time
import urlparse

from .pypi import Package


logger = logging.getLogger(__name__)


class Processor(object):

    def __init__(self, warehouse, pypi, store, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

        self.warehouse = warehouse
        self.pypi = pypi
        self.store = store

    def get_and_update_or_create_version(self, release, project):
        version_data = release.serialize()
        version_data.update({"project": project})

        version, created = self.warehouse.versions.objects.get_or_create(project__name=project.name, version=release.version, show_yanked=True, defaults=version_data)

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
        file_data = distribution.serialize()
        file_data.update({"version": version})

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

    def update_files(self, release, version):
        # Determine if any files need to be deleted
        warehouse_files = set([f.filename for f in version.files])
        local_files = set([x.filename for x in release.files])
        deleted = warehouse_files - local_files

        # Delete any files that need to be deleted
        if deleted:
            for filename in deleted:
                logger.info("Deleting the file '%s' from '%s' version '%s'", filename, release.name, release.version)

            self.warehouse.files.objects.filter(filename__in=deleted).delete()

        return [self.get_and_update_or_create_file(release, version, distribution) for distribution in release.files]

    def update(self, name, version=None, timestamp=None, action=None, matches=None):
        package = Package(self.pypi, name, version)

        # Process the Name
        project, _ = self.warehouse.projects.objects.get_or_create(name=name)

        for release in package.releases():
            if "/" in release.version:
                # We cannot accept versions with a / in it.
                logger.error("Skipping '%s' version '%s' because it contains a '/'", release.name, release.version)
                continue

            if not release.changed(self.store.get("pypi:process:%s:%s" % (release.name, release.version))):
                logger.info("Skipping '%s' version '%s' because it has not changed", release.name, release.version)
                continue

            logger.info("Syncing '%s' version '%s'", release.name, release.version)

            version = self.get_and_update_or_create_version(release, project)
            self.update_files(release, version)

            self.store.set("pypi:process:%s:%s" % (release.name, release.version), release.hash())

    def delete(self, name, version, timestamp, action, matches):
        filename = None

        if action == "remove":
            if version is None:
                obj = self.warehouse.projects.objects.filter(name=name)
                logger.info("Deleting '%s'", name)
            else:
                obj = self.warehouse.versions.objects.filter(project__name=name, version=version)
                logger.info("Deleting '%s' version '%s'", name, version)
        elif action.startswith("remove file"):
            filename = matches.groups()[0]
            obj = self.warehouse.files.objects.filter(filename=filename)
            logger.info("Deleting '%s' version '%s' filename '%s'", name, version, filename)
        else:
            raise RuntimeError("Unknown Action passed to delete()")

        try:
            obj = obj.get()
        except obj.resource.DoesNotExist:
            return

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

        if not self.store.get("pypi:since"):
            # This is the first time we've ran so we need to do a bulk import
            raise RuntimeError(" Cannot process changes with no value for the last successful run.")

        current = datetime.datetime.utcnow().replace(microsecond=0)

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

        changes = self.pypi.changelog(since)

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

        # Hijack the warehouse session and url
        last_modified_url = urlparse.urljoin(self.warehouse.url, "/last-modified")
        resp = self.warehouse.session.post(last_modified_url, {"date": current.isoformat()})
        resp.raise_for_status()

        self.store.set("pypi:since", time.mktime(current.timetuple()))

        logger.info("Finished changed projects synchronization")

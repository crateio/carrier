from __future__ import absolute_import
from __future__ import division


import bz2
import csv
import logging
import logging.config
import io
import time
import urlparse

import lxml.html
import redis
import requests
import slumber
import yaml

from apscheduler.scheduler import Scheduler

from conveyor.processor import Processor, get_key

# @@@ Switch all Urls to SSL
# @@@ Switch to better exception classes


logger = logging.getLogger(__name__)


class Conveyor(object):

    def __init__(self, config_file=None, *args, **kwargs):
        super(Conveyor, self).__init__(*args, **kwargs)

        if config_file is None:
            config_file = "config.yml"

        with open(config_file) as f:
            self.config = yaml.safe_load(f.read())

        logging.config.dictConfig(self.config["logging"])

        self.redis = redis.StrictRedis(**self.config.get("redis", {}).get("connection", {}))

    def run(self):
        self.scheduler = Scheduler()

        if self.config["conveyor"].get("schedule", {}).get("packages", {}):
            self.scheduler.add_interval_job(self.packages, **self.config["conveyor"]["schedule"]["packages"])

        if self.config["conveyor"].get("schedule", {}).get("downloads", {}):
            self.scheduler.add_interval_job(self.downloads, **self.config["conveyor"]["schedule"]["downloads"])

        self.scheduler.start()

        try:
            while True:
                time.sleep(999)
        except KeyboardInterrupt:
            logger.info("Shutting down Conveyor...")
            self.scheduler.shutdown(wait=False)

    def packages(self):
        if not self.redis.get(get_key(self.config.get("redis", {}).get("prefix", None), "pypi:since")):
            # This is the first time we've ran so we need to do a bulk import
            raise Exception(" Cannot process changes with no value for the last successful run.")

        warehouse = slumber.API(
                        self.config["conveyor"]["warehouse"]["url"],
                        auth=(
                            self.config["conveyor"]["warehouse"]["auth"]["username"],
                            self.config["conveyor"]["warehouse"]["auth"]["password"],
                        )
                    )

        processor = Processor(
                        index=self.config["conveyor"]["index"],
                        warehouse=warehouse,
                        store=self.redis,
                        store_prefix=self.config.get("redis", {}).get("prefix", None)
                    )

        processor.process()

    def downloads(self):
        session = requests.session()

        warehouse = slumber.API(
                        self.config["conveyor"]["warehouse"]["url"],
                        auth=(
                            self.config["conveyor"]["warehouse"]["auth"]["username"],
                            self.config["conveyor"]["warehouse"]["auth"]["password"],
                        )
                    )

        # Get a listing of all the Files
        resp = session.get(self.config["conveyor"]["stats"])
        resp.raise_for_status()

        html = lxml.html.fromstring(resp.content)
        urls = [(urlparse.urljoin(self.config["conveyor"]["stats"], x), x) for x in html.xpath("//a/@href")]

        for url, statfile in urls:
            if not url.endswith(".bz2"):
                continue

            date = statfile[:-4]
            year, month, day = date.split("-")

            # @@@ Check Modified
            last_modified_key = get_key(self.config.get("redis", {}).get("prefix", ""), "pypi:download:last_modified:%s" % url)
            last_modified = self.redis.get(last_modified_key)

            headers = {"If-Modified-Since": last_modified} if last_modified else None

            resp = session.get(url, headers=headers, prefetch=True)

            if resp.status_code == 304:
                logger.info("Skipping %s, it has not been modified since %s", statfile, last_modified)
                continue

            resp.raise_for_status()

            logger.info("Computing download counts from %s", statfile)

            data = bz2.decompress(resp.content)
            csv_r = csv.DictReader(io.BytesIO(data), ["project", "filename", "user_agent", "downloads"])

            for row in csv_r:
                row["date"] = date

                # See if we have a Download object for this yet
                try:
                    downloads = warehouse.downloads.get(project=row["project"], filename=row["filename"], date__year=year, date__month=month, date__day=day, user_agent=row["user_agent"])
                except Exception as e:
                    import pdb; pdb.set_trace()
                    raise

                if downloads["meta"]["total_count"] == 1:
                    warehouse.downloads(downloads["objects"][0]["id"]).put(row)
                elif downloads["meta"]["total_count"] == 0:
                    warehouse.downloads.post(row)
                else:
                    RuntimeError("There are More than 1 Download items returned")

            if "Last-Modified" in resp.headers:
                self.redis.set(last_modified_key, resp.headers["Last-Modified"])
            else:
                self.redis.delete(last_modified_key)

            break

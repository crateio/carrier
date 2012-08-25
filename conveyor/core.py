from __future__ import absolute_import
from __future__ import division

import logging
import logging.config
import os
import time

import forklift
import redis
import requests
import xmlrpc2.client

from apscheduler.scheduler import Scheduler

from .config import Config, defaults
from conveyor.processor import Processor


logger = logging.getLogger(__name__)


class Conveyor(object):

    def __init__(self, *args, **kwargs):
        super(Conveyor, self).__init__(*args, **kwargs)

        # Get configuration
        self.config = Config(os.path.dirname(__file__))
        self.config.from_object(defaults)
        self.config.from_envvar("CONVEYOR_CONF")

        # Initalize app
        logging.config.dictConfig(self.config["LOGGING"])

        self.redis = redis.StrictRedis(**dict([(k.lower, v) for k, v in self.config["REDIS"].items()]))
        self.scheduler = None

        wsession = requests.session(auth=(
                        self.config["WAREHOUSE_AUTH"]["USERNAME"],
                        self.config["WAREHOUSE_AUTH"]["PASSWORD"],
                    ))
        warehouse = forklift.Forklift(session=wsession)

        psession = requests.session(verify=self.config["PYPI_SSL_VERIFY"])
        pypi = xmlrpc2.client.Client(self.config["PYPI_URI"], session=psession)

        self.processor = Processor(warehouse, pypi, self.redis)

    def run(self):
        self.scheduler = Scheduler()

        if self.config["SCHEDULE"].get("packages") is not None:
            self.scheduler.add_interval_job(self.packages, **self.config["SCHEDULE"]["packages"])

        self.scheduler.start()

        try:
            while True:
                time.sleep(999)
        except KeyboardInterrupt:
            logger.info("Shutting down Conveyor...")
            self.scheduler.shutdown(wait=False)

    def packages(self):
        if not self.redis.get("pypi:since"):
            # This is the first time we've ran so we need to do a bulk import
            raise Exception(" Cannot process changes with no value for the last successful run.")

        self.processor.process()

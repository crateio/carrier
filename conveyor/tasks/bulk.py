import datetime
import logging
import time

import forklift
import requests
import xmlrpc2.client

from requests.exceptions import ConnectionError, HTTPError

from conveyor.core import Conveyor
from .pypi import Package
from conveyor.processor import Processor

logger = logging.getLogger(__name__)


# We ignore the last component as we cannot properly handle it
def get_jobs(last=0):
    try:
        current = time.mktime(datetime.datetime.utcnow().timetuple())

        app = Conveyor()

        logger.info("Current time is '%s'", current)

        warehouse = forklift.Forklift(
                        session=requests.session(
                                    auth=(
                                        app.config["conveyor"]["warehouse"]["username"],
                                        app.config["conveyor"]["warehouse"]["password"]
                                    )
                        )
                    )

        session = requests.session(verify=app.config["conveyor"].get("verify", True))

        processor = Processor(
                        index=app.config["conveyor"]["index"],
                        warehouse=warehouse,
                        session=session,
                        store=app.redis,
                    )

        names = set(processor.client.list_packages())

        for package in names:
            yield package
    except Exception as e:
        logger.exception(str(e))
        raise


def handle_job(name):
    try:
        tried = 0
        delay = 1

        while True:
            try:
                tried += 1

                app = Conveyor()

                warehouse = forklift.Forklift(
                                session=requests.session(
                                            auth=(
                                                app.config["conveyor"]["warehouse"]["username"],
                                                app.config["conveyor"]["warehouse"]["password"]
                                            )
                                )
                            )

                session = requests.session(verify=app.config["conveyor"].get("verify", True))

                client = xmlrpc2.client.Client(app.config["conveyor"]["index"], session=session)

                processor = Processor(
                                index=app.config["conveyor"]["index"],
                                warehouse=warehouse,
                                session=session,
                                store=app.redis,
                            )

                # Process the Name
                warehouse.projects.objects.get_or_create(name=name)

                package = Package(client, name)

                for release in package.releases():
                    processor.sync_release(release)

                break
            except (ConnectionError, HTTPError):
                # Attempt to process again if we have a connection error
                if tried >= 10:  # Try a max of 10 times
                    raise
                else:
                    # Wait a moment
                    time.sleep(delay)
                    delay = delay * 2
    except Exception as e:
        logger.exception(str(e))
        raise

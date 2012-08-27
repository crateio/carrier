from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import datetime
import logging
import time

from requests.exceptions import ConnectionError, HTTPError

from ..core import Carrier


logger = logging.getLogger(__name__)


# We ignore the last component as we cannot properly handle it
def get_jobs(last=0):
    current = time.mktime(datetime.datetime.utcnow().timetuple())

    print "Current time is", current

    app = Carrier()

    for package in set(app.processor.pypi.list_packages()):
        yield package


def handle_job(name):
    try:
        tried = 0
        delay = 1

        while True:
            try:
                tried += 1

                app = Carrier()
                app.processor.update(name)

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

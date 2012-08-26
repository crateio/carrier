Carrier: Warehouse and PyPI Synchronization
===========================================

Carrier is a BSD Licensed Python application for keeping a Warehouse instance
and PyPI synchronized.

Carrier utilizes the xmlrpc and pubhubsubbub API of PyPI to keep a Warehouse
instance up to date with PyPI. It can also utilize webhooks from Warehouse to
propagate changes made in Warehouse to PyPI. It enables a clean separation of
Warehouse and PyPI.

Carrier is part of the Crate project, a set of applications, tools, and libraries
for creating, distributing, and installing Python packages in a secure, efficient,
and reliable way.


Resources
=========

* Documentation_
* `Bug Tracker`_
* Code_
* IRC_ *(irc.freenode.net, #crate)*

.. _Documentation: https://docs.crate.io/carrier/
.. _`Bug Tracker`: https://github.com/crateio/carrier/issues
.. _Code: https://github.com/crateio/carrier/
.. _IRC: http://webchat.freenode.net?channels=crate

Contribute
==========

1. Check for open issues or open a new issue to start a discussion around a feature
   idea or a bug.
2. Fork `the repository`_ on GitHub and create a branch off of the master branch
   to make your changes too.
3. Write a test which shows that the bug was fixed or that the feature works as expected.
4. Send a pull request and bug the maintainer until it gets merged and published.

.. _`the repository`: https://github.com/crateio/carrier/

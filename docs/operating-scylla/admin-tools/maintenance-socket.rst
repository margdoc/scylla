Maintenance socket
==================

It enables interaction with the node through CQL protocol without authentication. It gives full-permission access.
The maintenance socket is available by Unix domain socket with file permissions `755`, thus it is not accessible from outside of the node and from other POSIX groups on the node.
It is created before the node joins the cluster.

To set up the maintenance socket, use the `maintenance-socket` option when starting the node.

* If set to `ignore` maintenance socket will not be created.
* If set to `workdir` maintenance socket will be created in `<node's workdir>/cql.maintenance`.
* Otherwise maintenance socket will be created in the specified path.

The default value is `workdir`.

Connect to maintenance socket
-----------------------------

With python driver
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.connection import UnixSocketEndPoint
    
    cluster = Cluster([UnixSocketEndPoint("<node's workdir>/cql.maintenance")])
    session = cluster.connect()

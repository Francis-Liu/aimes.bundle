``Bundle``
=========

'bundle_manager' is the major module provided by 'bundle'

'bundle' depends on the following third party library to run:
  paramiko  (A Python SSHv2 protocal library)
  Pyro4*    (Python Remote Objects, needed only when using Bundle as a remote object)

Usage example:
**************
Before using bundle_manager, user needs to create a configuration file.
  src/bundle/example/bundle_credentials.txt is a template configuration file

There are two ways to use bundle_manager.
The first way is to directly import bundle_manager as a library.
  src/bundle/example/bundle_cml.py shows an example of using bundle_manager in
  this way.
The second way is to launch bundle_manager as a deamon, register it with Pyro4 
as a remote object. User program should query Pyro4 with bundle_manager's uri
to get a reference to the remote object and call thie remote object's functions
  src/bundle/example/bundle_cmlPyro.py shows an example of using bundle_manager
  in this way.

A screen copy of results on india.futuregrid.org (using aboved mentioned first way)
**************
fengl@exa (/home/grad03/fengl/DOEProj) % ls bundle_cml.py bundle
bundle_cml.py*

bundle:
api/  bundle_credentials.txt  db/  impl/  __init__.py  __init__.pyc  tools/
fengl@exa (/home/grad03/fengl/DOEProj) % cat bundle/bundle_credentials.txt 
#bundle cluster credential file
#each line contains credential of a cluster, used for launching a remote connection to the cluster
#accepted credential fields include: hostname, port, username, password, key_filename
finished_job_trace=/home/grad03/fengl/DOEProj/bundle/db
cluster_type=moab hostname=india.futuregrid.org username=liux2102 key_filename=/home/grad03/fengl/.ssh/id_rsa h_flag=True
#cluster_type=moab hostname=xray.futuregrid.org username=liux2102 key_filename=/home/grad03/fengl/.ssh/id_rsa
#cluster_type=moab hostname=hotel.futuregrid.org username=liux2102 key_filename=/home/grad03/fengl/.ssh/id_rsa
#cluster_type=moab hostname=sierra.futuregrid.org username=liux2102 key_filename=/home/grad03/fengl/.ssh/id_rsa
#cluster_type=moab hostname=alamo.futuregrid.org username=liux2102 key_filename=/home/grad03/fengl/.ssh/id_rsa
fengl@exa (/home/grad03/fengl/DOEProj) % ~/virtualenv/bin/python bundle_cml.py
Enter command: loadc bundle/bundle_credentials.txt
2013-10-29 17:11:39,251 india.futuregrid.org india.futuregrid.org INFO     __init__:112  Connected to india.futuregrid.org
Enter command: list
['india.futuregrid.org']
Enter command: showc india.futuregrid.org
{'state': 'Up', 'num_procs': 248, 'pool': {'compute': {'np': 8, 'num_procs': 168, 'num_nodes': 21}, 'b534': {'np': 8, 'num_procs': 8, 'num_nodes': 1}, 'delta': {'np': 12, 'num_procs': 72, 'num_nodes': 6}}, 'queue_info': {'bravo': {'started': 'True', 'queue_name': 'bravo', 'enabled': 'True', 'pool': 'bravo', 'max_walltime': 86400}, 'batch': {'started': 'True', 'queue_name': 'batch', 'enabled': 'True', 'pool': 'compute', 'max_walltime': 86400}, 'long': {'started': 'True', 'queue_name': 'long', 'enabled': 'True', 'pool': 'compute', 'max_walltime': 604800}, 'delta-long': {'started': 'True', 'queue_name': 'delta-long', 'enabled': 'True', 'pool': 'delta', 'max_walltime': 604800}, 'delta': {'started': 'True', 'queue_name': 'delta', 'enabled': 'True', 'pool': 'delta', 'max_walltime': 86400}, 'b534': {'started': 'True', 'queue_name': 'b534', 'enabled': 'True', 'pool': 'b534', 'max_walltime': 604800}, 'ib': {'started': 'True', 'queue_name': 'ib', 'enabled': 'True', 'pool': 'compute', 'max_walltime': 86400}, 'interactive': {'started': 'True', 'queue_name': 'interactive', 'enabled': 'True', 'pool': 'compute', 'max_walltime': 86400}}, 'num_nodes': 28}
Enter command: showw india.futuregrid.org
{'free_procs': 208, 'per_pool_workload': {'compute': {'free_procs': 128, 'free_nodes': 16, 'alive_nodes': 21, 'busy_nodes': 5, 'np': 8, 'busy_procs': 40, 'alive_procs': 168}, 'b534': {'free_procs': 8, 'free_nodes': 1, 'alive_nodes': 1, 'busy_nodes': 0, 'np': 8, 'busy_procs': 0, 'alive_procs': 8}, 'delta': {'free_procs': 72, 'free_nodes': 6, 'alive_nodes': 6, 'busy_nodes': 0, 'np': 12, 'busy_procs': 0, 'alive_procs': 72}}, 'free_nodes': 23, 'alive_nodes': 28, 'busy_nodes': 5, 'busy_procs': 40, 'alive_procs': 248}
Enter command: quit
2013-10-29 17:12:17,222 india.futuregrid.org india.futuregrid.org DEBUG    close:1003 close
2013-10-29 17:12:17,222 india.futuregrid.org india.futuregrid.org DEBUG    run:607  received "close" command
cmd_line_loop finish

A screen copy of results on india.futuregrid.org (using aboved mentioned second way)
**************
fengl@exa (/home/grad03/fengl) % ~/virtualenv/bin/python -m Pyro4.naming
/home/grad03/fengl/virtualenv/local/lib/python2.7/site-packages/Pyro4/core.py:167: UserWarning: HMAC_KEY not set, protocol data may not be secure
  warnings.warn("HMAC_KEY not set, protocol data may not be secure")
Not starting broadcast server for localhost.
NS running on localhost:9090 (127.0.0.1)
URI = PYRO:Pyro.NameServer@localhost:9090



#Open another terminal
fengl@exa (/home/grad03/fengl/DOEProj) % ~/virtualenv/bin/python bundle/impl/bundle_manager.py -D -c bundle/bundle_credentials.txt 
daemon mode
2013-10-29 16:05:12,393 india.futuregrid.org india.futuregrid.org INFO     __init__:112  Connected to india.futuregrid.org
2013-10-29 16:05:12,393 INFO:india.futuregrid.org:bundle_agent.py:112:Connected to india.futuregrid.org
/home/grad03/fengl/virtualenv/local/lib/python2.7/site-packages/Pyro4/core.py:167: UserWarning: HMAC_KEY not set, protocol data may not be secure
  warnings.warn("HMAC_KEY not set, protocol data may not be secure")
Object <__main__.BundleManager object at 0x1c59f90>:
    uri = PYRO:obj_9262a45a566a46f39c4fad5288fbf9ae@localhost:41540
    name = BundleManager
Pyro daemon running.



#Check bundle_manager has successfully registered itself as a remote object to Pyro4
fengl@exa (/home/grad03/fengl) % ~/virtualenv/bin/python -m Pyro4.nsc list
/home/grad03/fengl/virtualenv/local/lib/python2.7/site-packages/Pyro4/core.py:167: UserWarning: HMAC_KEY not set, protocol data may not be secure
  warnings.warn("HMAC_KEY not set, protocol data may not be secure")
--------START LIST 
BundleManager --> PYRO:obj_9262a45a566a46f39c4fad5288fbf9ae@localhost:41540
Pyro.NameServer --> PYRO:Pyro.NameServer@localhost:9090
--------END LIST


``AIMES``
=========

AIMES is a DOE ASCR funded collaborative project between the RADICAL
group at Rutgers, University of Minnesota, and the Computation Institute
at the University of Chicago, that will explore the role of abstractions
and integrated middleware to support science at extreme scales. AIMES
will co-design middleware from an application and infrastructure
perspective. AIMES will provide abstractions for compute, data and
network, integrated across multiple levels to provide an interoperable,
extensible and scalable middleware stack to support extreme-scale
science.

AIMES is funded by DOE ASCR under grant numbers: DE-FG02-12ER26115,
DE-SC0008617, and DE-SC0008651

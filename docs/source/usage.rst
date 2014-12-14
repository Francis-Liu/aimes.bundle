
.. _chapter_usage:

***************************
Starting the Bundle Manager
***************************


.. code-block:: shell

    # aimes-bundle-manager --help

    usage  : bin/aimes-bundle-manager -c <config> [-m <mode>] [-u url] [-v] [-h]
    example: bin/aimes-bundle-manager -c bundle.cfg -m mongodb -u mongodb://localhost/

    options:
        -c --config-file
            location of the bundle configuration file (mandatory)
       
        -m --mode
            specify connectivity mode: once, mongodb or pyro4 (default: once)
       
        -u --url
            specify communication endpoint for mongodb 
            default: mongodb://localhost:27017/bundle_v0_1/
       
        -v --verbose
            verbose operation (default: no)
       
        -h --help
            print help (default: no)


    modes:

        once   : the bundle manager will start agents according to the
                 config file, collect the information gathered by the
                 agents, and print them on stdout.
        mongodb: the bundle manager will start agents according to the
                 config file, collect the information gathered by the
                 agents, and store it in a mongodb for later query by
                 clients.
        pyro4  : the bundle manager will start agents according to the
                 config file, collect the information gathered by the
                     agents, and provide it as a pyro4 shared object structure.

            For both mongodb and pyro4 modes, the agent will got into background
            operation and will recollect live information from the agents every
            60 seconds, ie. it will become a dameon.

    .


.. code-block:: shell

    # aimes-bundle-query --help

    usage  : bin/aimes-bundle-query -c <config> [-m <mode>] [-d] [-v]
    example: bin/aimes-bundle-query -c bundle.cfg -m mongodb -u mongodb://localhost/

    options:
        -c --config-file
            location of the bundle configuration file (mandatory)
       
        -m --mode
            specify query mode: direct or mongodb (default: direct)
       
        -u --url
            specify communication endpoint for mongodb 
            default: mongodb://localhost:27017/bundle_v0_1/
       
        -o --origin
            the source host for bandwidth queries
            default: None
       
        -v --verbose
            verbose operation (default: no)
       
        -h --help
            print help (default: no)


    modes:

        direct : the bundle module will start agents according to the
                 config file, collect the information gathered by the
                 agents, and print them on stdout.
        mongodb: the bundle manager will quert the specified mongodb,
                 and dump the data found there.


    .



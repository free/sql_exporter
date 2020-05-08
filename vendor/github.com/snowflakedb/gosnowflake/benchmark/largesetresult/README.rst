********************************************************************************
Benchmark Large Result Set
********************************************************************************

This folder includes a benchmark test case for "Large Result set", which refers 
to the query result more than 100 MB as Snowflake clients fetches the first small 
chunk of data (~100MB) from Snowflake DB and downloads the rest of data chunks 
from AWS S3 bucket.

Profiling
=========

Using Go's profilers, you may see CPU and memory usage on each function/method. 
This command instruments CPU and memory usage and save them into files.

.. code-block:: bash

    SNOWFLAKE_TEST_ACCOUNT=<your_account> \
    SNOWFLAKE_TEST_USER=<your_user> \
    SNOWFLAKE_TEST_PASSWORD=<your_password> \
    make profile

Check CPU usage on the web browser:

.. code-block:: bash

    go tool pprof largesetresult.test cpu.out
    (pprof) web

Check memory usage on the web browser:

.. code-block:: bash

    go tool pprof -alloc_space largesetresult.test mem.out
    (pprof) web

Tracing
=======

Using Go's trace tool, you may see all of the goroutine's activity with timeline.

.. code-block:: bash

    SNOWFLAKE_TEST_ACCOUNT=<your_account> \
    SNOWFLAKE_TEST_USER=<your_user> \
    SNOWFLAKE_TEST_PASSWORD=<your_password> \
    make trace

Check goroutine's activities on web browser.

.. code-block:: bash

    go tool trace largesetresult.test trace.out



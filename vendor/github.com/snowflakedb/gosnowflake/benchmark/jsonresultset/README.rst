********************************************************************************
Benchmark Large Result Set
********************************************************************************

This folder includes a benchmark test case for "JSON Result Set", which refers
to a query result of more than 100 MB of JSON objects. This differs from the "Large
Result Set" case, since it benchmarks large strings with many escaped characters.

Profiling
=========

Using Go's profilers, you may see CPU and memory usage on each function/method. 
This command instruments CPU and memory usage and save them into files.

.. code-block:: bash

    SNOWFLAKE_TEST_ACCOUNT=<your_account> \
    SNOWFLAKE_TEST_USER=<your_user> \
    SNOWFLAKE_TEST_PASSWORD=<your_password> \
    SNOWFLAKE_TEST_CUSTOME_JSON_DECODER_ENABLE=<true/false> \
    SNOWFLAKE_TEST_MAX_CHUNK_DOWNLOAD_WORKERS=<number_of_workers> \
    make profile

Check CPU usage on the web browser:

.. code-block:: bash

    go tool pprof jsonresultset.test cpu.out
    (pprof) web

Check memory usage on the web browser:

.. code-block:: bash

    go tool pprof -alloc_space jsonresultset.test mem.out
    (pprof) web

Note adjust SNOWFLAKE_TEST_CUSTOME_JSON_DECODER_ENABLE and SNOWFLAKE_TEST_MAX_CHUNK_DOWNLOAD_WORKERS to

Tracing
=======

Using Go's trace tool, you may see all of the goroutine's activity with timeline.

.. code-block:: bash

    SNOWFLAKE_TEST_ACCOUNT=<your_account> \
    SNOWFLAKE_TEST_USER=<your_user> \
    SNOWFLAKE_TEST_PASSWORD=<your_password> \
    SNOWFLAKE_TEST_CUSTOME_JSON_DECODER_ENABLE=<true/false> \
    SNOWFLAKE_TEST_MAX_CHUNK_DOWNLOAD_WORKERS=<number_of_workers> \
    make trace

Check goroutine's activities on web browser.

.. code-block:: bash

    go tool trace jsonresultset.test trace.out



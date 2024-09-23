
## Postgres optimizer plugin


* Based on pg 17beta1, other versions have not been tested, please modify as needed if issues arise.


0. The code is based on modifications from https://github.com/greenplum-db/gporca-archive.git, and has synchronized the latest code of orca from gpdb.

1. If you are only interested in the distributed optimization of Orca, checkout to `origin_orca`, all tests passed. It can run independently without pg, and most native Orca functions are available, except for solving compilation issues, there are almost no significant changes.

2. Now orca has been restructured into a pg plugin, you can run and debug this code as a plugin. However, due to some necessary modifications, it is currently unable to generate distributed execution plans. But if you are only interested in the optimizer itself, it still has some use. Refer to test/schedule for current SQL tests.


### build

1. Clone the code.
2. Checkout to the commit according to your needs.

    ```bash
    cmake  -Bbuild -G Ninja
    cmake --build build
    ```

* you can use `-DENABLE_COVERAGE=TRUE` to collect code coverage.
    ```
    lcov -d . -c -o coverage.info
    lcov --summary coverage.info
    ```
* ensure you can use pg_config normally, then use `cmake --build build --target install` to install the plugin.
* Currently, use `pg_orca.enable_orca` to control whether to enable the orca optimizer, it is turned off by default, and needs to be manually enabled. You can set `pg_orca.enable_orca` to enable it.
* Configure `shared_preload_libraries = 'pg_orca'`, or manually `load 'pg_orca.so';`
* test depended on pg_tpch and pg_tpcds, you can find them in my repository

### Research code, do not use in production

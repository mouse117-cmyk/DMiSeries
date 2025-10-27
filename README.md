# DMiSeries

## Server

### Dependencies

```
$ sudo apt-get install libyaml-cpp-dev cmake build-essential libboost-all-dev google-perftools libprotobuf-dev protobuf-compiler libgoogle-perftools-dev libsnappy-dev
$ git clone https://github.com/jemalloc/jemalloc.git
$ cd jemalloc
$ autoconf
$ ./configure
$ make
$ sudo make install
```

### Build

```
$ cd TempusCStore
$ mkdir build && cd build && cmake ..
$ make RemoteStorage -j32
```

### Run

The default port number is `9966` and can be modified in the `RemoteDB.h` file. The service needs to be restarted before each test to prevent old data from interfering with new queries. You can start the service in the following way:

```
$ ./RemoteStorage
```

## Test

### Build

```
$ make DMiSeries_test
```

### Run

Before running, you need to configure the server's IP address and port number in the `server_config.json` file in the root directory.

Example

```
{
  "ips": [
    "192.168.1.102", "192.168.1.103"
  ],
  "ports": [
    9967, 9968
  ]
}
```

You can test the read and write performance of the DMiSeries like this:

```
$ ./DMiSeries_test <row_count> <per_insert_num> <num_q> --gtest_filter=AlgorithmTest.TestDevopsSmall
```

- [row_count] total Number of rows of data read

- [per_insert_num] The number of rows read for each batch during batch insertion

- [num_q] Total number of query nodes


Example

```
$ ./DMiSeries_test 100000 100000 1000 --gtest_filter=AlgorithmTest.TestDevopsSmall
```

You can test the standard deviation of the data distribution like this:

```
$ ./DMiSeries_test <row_count> <times> <num_q> --gtest_filter=AlgorithmTest.TestSD
```

- [row_count] total Number of rows of data read

- [times] Number of tests

- [num_q] Total number of query nodes


Example

```
$ ./DMiSeries_test 10000 10 1000 --gtest_filter=AlgorithmTest.TestSD
```

## Baselines

ForestTI: [https://github.com/naivewong/forestti](https://github.com/naivewong/forestti)

TimeUnion: [https://github.com/naivewong/timeunion](https://github.com/naivewong/timeunion)

Prometheus tsdb: [https://github.com/prometheus-junkyard/tsdb](https://github.com/prometheus-junkyard/tsdb)

Cortex: [https://github.com/cortexproject/cortex](https://github.com/cortexproject/cortex)

## Datasets

IoT in TSBS: [https://github.com/timescale/tsbs](https://github.com/timescale/tsbs)

TSM-Bench: [https://github.com/eXascaleInfolab/TSM-Bench](https://github.com/eXascaleInfolab/TSM-Bench)

DBPA: [https://github.com/hjhhsy120/DBPA](https://github.com/hjhhsy120/DBPA)

IMAD-DS: [https://dcase-repo.github.io/dcase_datalist/datasets/anomalous/imad-ds.html](https://dcase-repo.github.io/dcase_datalist/datasets/anomalous/imad-ds.html)
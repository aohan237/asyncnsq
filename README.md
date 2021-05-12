# asyncnsq
[![Downloads](https://pepy.tech/badge/asyncnsq)](https://pepy.tech/project/asyncnsq)
[![PyPI version](https://badge.fury.io/py/asyncnsq.svg)](https://badge.fury.io/py/asyncnsq)

## async nsq with asyncio

**if you dont like the pynsq(which use tornado) way to interact with nsq, then this library may be suitable for you**

you can use this library as the common way to write things

## Important

* #### from version 1.0.0 asyncnsq  has a break change in api

* #### it is not stable yet

* #### you may want to use stable " pip install asyncnsq==0.4.5"

## Features

--------------

### Http Client

* support all the method nsq http supplied

### Tcp Client

#### Connection

* low level connection.

#### Reader

* reader from both lookupd for auto finding nsqd

* list of known nsqd but they can not use together.

* above two can't use together

#### Writer

* all the common method for nsqd writer

## Next Features

### different reader protocol support
### more doc to write
### more tests


## Install

--------------

pip install asyncnsq

## Usage examples

--------------

All you need is a loop, then enjoy. you can refer to examples, as well.

Consumer:

```python
from asyncnsq import create_reader
from asyncnsq.utils import get_logger

loop = asyncio.get_event_loop()
async def go():
    try:
        reader = await create_reader(
            nsqd_tcp_addresses=['127.0.0.1:4150'],
            max_in_flight=200)
        await reader.subscribe('test_async_nsq', 'nsq')
        async for message in reader.messages():
            print(message.body)
            await message.fin()
    except Exception as tmp:
        self.logger.exception(tmp)
loop.run_until_complete(go())
```

Producer:
```python
from asyncnsq import create_writer
loop = asyncio.get_event_loop()
async def go():
    writer = await create_writer(host='127.0.0.1', port=4150,
                                       heartbeat_interval=30000,
                                       feature_negotiation=True,
                                       tls_v1=True,
                                       snappy=False,
                                       deflate=False,
                                       deflate_level=0,
                                       loop=loop)
    for i in range(100):
        await writer.pub('test_async_nsq', 'test_async_nsq:{i}'.format(i=i))
        await writer.dpub('test_async_nsq', i * 1000,
                                'test_delay_async_nsq:{i}'.format(i=i))
loop.run_until_complete(go())
```

Requirements
------------

* Python_ 3.6+  https://www.python.org
* nsq_  http://nsq.io

* python-snappy
    1. ubuntu:
        - sudo apt-get install libsnappy-dev
        - pip install python-snappy
    2. centos:
        - sudo yum install snappy-devel
        - pip install python-snappy
    3. mac:
        - brew install snappy # snappy library from Google
        - CPPFLAGS="-I/usr/local/include -L/usr/local/lib" pip install python-snappy


Running Tests
-------------

1. install nsq requirements
    - install nsq
        https://nsq.io/deployment/installing.html

2. install requirements (in a virtual environment)
    - `pip install aiohttp python-snappy`
    - `pip install pytest`  dev test package if you want autotest support

3. run the auth server in a separate terminal session
    - `python -m aiohttp.web -H localhost -P 8080 asyncnsq.http.auth:create_dev_auth_server`

4. run nsq in separate terminal sessions
    - if you've built nsq through `make`, `cd` into the `build` directory of nsq
    - `./nsqlookupd`
    - `./nsqd --lookupd-tcp-address=localhost:4160 -auth-http-address=localhost:8080`

5. run tests
    - `python runtests.py`

    example output:

    ```
    $ python runtests.py test_reader_and_writer
    decorator test_01_writer (tests.test_reader_and_writer.NsqTest) <_UnixSelectorEventLoop running=False closed=False debug=False> () {}
    .decorator test_02_reader (tests.test_reader_and_writer.NsqTest) <_UnixSelectorEventLoop running=False closed=False debug=False> () {}
    .
    ----------------------------------------------------------------------
    Ran 2 tests in 0.260s

    OK
    ```

    - `pytest` just pytest will do all the trick

    ```
    $ pytest -k test_reader_and_writer
    ------------------------------------------------------------------------------ live log call    -------------------------------------------------------------------------------
    DEBUG    asyncnsq.tcp:connection.py:82 execute command b'IDENTIFY\n\x00\x00\x00\x1d {"feature_negotiation": true}'
    DEBUG    asyncnsq.tcp:connection.py:239 got nsq data: (0, b'{"max_rdy_count":2500,"version":"1.2.0",    "max_msg_timeout":900000,"msg_timeout":60000,"tls_v1":false,"deflate":false,"deflate_level":6,  "max_deflate_level":6,"snappy":false,"sample_rate":0,"auth_required":false,"output_buffer_size":16384,    "output_buffer_timeout":250}')
    DEBUG    asyncnsq.tcp:connection.py:208 Task is canceled
    SKIPPED (no auth    enabled)                                                                                                                                              [ 75%]
    tests/test_reader_and_writer.py::NsqTest::test_04_reader_fail_missing_secret
    ------------------------------------------------------------------------------ live log call    -------------------------------------------------------------------------------
    DEBUG    asyncnsq.tcp:connection.py:82 execute command b'IDENTIFY\n\x00\x00\x00\x1d {"feature_negotiation": true}'
    DEBUG    asyncnsq.tcp:connection.py:239 got nsq data: (0, b'{"max_rdy_count":2500,"version":"1.2.0",    "max_msg_timeout":900000,"msg_timeout":60000,"tls_v1":false,"deflate":false,"deflate_level":6,  "max_deflate_level":6,"snappy":false,"sample_rate":0,"auth_required":false,"output_buffer_size":16384,    "output_buffer_timeout":250}')
    DEBUG    asyncnsq.tcp:connection.py:208 Task is canceled
    SKIPPED (no auth    enabled)                                                                                                                                              [100%]

    =============================================================== 2 passed, 2 skipped, 39 deselected in 0.    65s ================================================================

    ```


License
-------

The asyncnsq is offered under MIT license.

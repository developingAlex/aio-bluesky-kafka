# aio bluesky kafka

## About

The [bluesky-kafka](https://pypi.org/project/bluesky-kafka/) package knows how
to properly decode recieved kafka messages emitted by
[bluesky](https://pypi.org/project/bluesky/) but operates in a blocking
fashion.

This package puts that code into a separate thread so that it doesn't block
the rest of your code (intention being asyncio code) and then you can supply
a callback coroutine function that it will call with every bluesky document
it gets.

## How to use

setup and initialisation:

```
import asyncio
from aio_bluesky_kafka import msg_handler

KAFKA_TOPIC = "queueserver"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_CONSUMER_GROUP_PREFIX = "group"

shutdown_event_object = asyncio.Event(loop=asyncio.get_event_loop())

async def handle_doc_cb(name, doc):
    print("handling a doc!")
    print(f"this is a {name} doc. This is its contents:")
    print(doc)

msg_handler_task = asyncio.ensure_future(
    msg_handler(
        handle_doc_cb,
        shutdown_event_object,
        KAFKA_TOPIC,
        KAFKA_CONSUMER_GROUP_PREFIX,
        KAFKA_BOOTSTRAP_SERVERS
    )
)

```

at this point every time a new kafka message is received, your handle_doc_cb
callback function will be executed with the document.

if you want to stop your app gracefully you can arrange for the handler to
also stop gracefully by setting the shutdown event like this:

```
shutdown_event_object.set()
```


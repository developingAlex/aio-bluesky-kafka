import asyncio
import threading
import queue
from bluesky_kafka import BlueskyConsumer
import functools


def bc_thread_func(msg_queue,
                   stop_it,
                   kafka_topic,
                   kafka_group,
                   kafka_bootstrap_servers):
    """ This runs as a separate thread, because listening to kafka is
    blocking """
    def add_to_queue(name, doc):
        msg_queue.put([name, doc])

    def continue_polling():
        return stop_it.isSet() is False

    bs_consumer = BlueskyConsumer(
        topics=[kafka_topic],
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=kafka_group,
        consumer_config={"auto.offset.reset": "latest"},
        process_document=lambda consumer, topic, name, doc: add_to_queue(name, doc))

    bs_consumer.start(continue_polling=continue_polling)  # blocking !!

    print("reached end of bc_thread_func")


async def queue_watcher(queue_obj, cb):
    while True:
        # running in executor allows us to wait on the queue to have something
        # put into it IN A NON BLOCKING WAY, but the issue with that is that in
        # order to achieve this we can effectively think of the waiting-on-the-
        # queue code to be running in a thread, which brings up the issue of how
        # when everything around us is being wrapped up during a shutdown event
        # or similar, can we arrange for the thread started by the
        # run_in_executor to get the message that it was meant to stop watching
        # the queue and wrap up as well?
        # based on the answers here:
        # https://stackoverflow.com/questions/26413613/asyncio-is-it-possible-to-cancel-a-future-been-run-by-an-executor
        # I've decided to solve it as I have in the past, by writing logic that
        # has it check if the item it pulls from the queue is a special "stop"
        # kind of item and if so, wraps up.., if not, carries on handling it
        # as a normal item.
        doc = await asyncio.get_event_loop().run_in_executor(
            None,
            functools.partial(queue_obj.get, block=True))
        # here is where we first check that it's not a special "stop" item left
        # for us by our managing code:
        if doc == "finish up now you hear?":
            print("queue_watcher stopping now as told")
            break
        await cb(doc[0], doc[1])


async def msg_handler(
        cb,
        finish_event,
        kafka_topic,
        kafka_group,
        kafka_bootstrap_servers):
    stop_it = threading.Event()
    # create queue,
    msg_queue = queue.Queue()
    # start the BCThread
    bc_thread = threading.Thread(name="BCThread",
                                 target=bc_thread_func,
                                 args=(msg_queue,
                                       stop_it,
                                       kafka_topic,
                                       kafka_group,
                                       kafka_bootstrap_servers))

    bc_thread.start()

    # now start up a coroutine to monitor the msg_queue, and reserve THIS
    # coroutine for monitoring for our shutdown event:
    queue_watcher_task = asyncio.ensure_future(queue_watcher(msg_queue, cb))

    # now we pause here, leaving the bc_thread Thread to run watching the kafka
    # queue, whenever it gets a message it will just add it to our internal
    # msg_queue Queue, and then also our queue_watcher coroutine is running
    # watching that msg_queue and whenever the bc_thread puts a document in
    # that queue, the queue_watcher coroutine will share it with the rest of
    # the (asyncio driven) application via calling the supplied callback
    # function.
    await finish_event.wait()
    # now we've been signalled to wrap it up

    # stop the bc_thread:
    stop_it.set()

    # stop the queue_watcher coroutine:
    msg_queue.put("finish up now you hear?")
    try:
        await queue_watcher_task
        print("queue_watcher task is now finished")
    except asyncio.CancelledError:
        print("queue_watcher task is now cancelled")
    bc_thread.join(2)
    if bc_thread.is_alive():
        print("timed out after 2 seconds waiting for bc_thread to wrap up!")
    else:
        print("bc_thread has now been stopped.")
    print("reached end of msg_handler")


async def main():
    """
    example usage
    """

    import os

    # configure how we can connect to kafka to get bluesky docs:
    kafka_topic = os.getenv('KAFKA_TOPIC', 'queueserver')
    kafka_consumer_group_prefix = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'group')
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    # define our callback:
    def my_callback(name, doc):
        print(' ')
        print(name)
        print(doc)
        print(type(doc))

    # create a shutdown event object:
    shutdown_event_object = asyncio.Event(loop=asyncio.get_event_loop())

    # kick it off!
    msg_handler_task = asyncio.ensure_future(
        msg_handler(
            my_callback,
            shutdown_event_object,
            kafka_topic,
            kafka_consumer_group_prefix,
            kafka_bootstrap_servers
        )
    )

    # simulate the rest of the program doing its thing for a bit...
    await asyncio.sleep(10)
    # during this time you should arrange for some bluesky messages to
    # be emitted to the kafka queue if you want to see any output.
    # then simulate normal shutdown:
    shutdown_event_object.set()
    await msg_handler_task
    print("reached end of main()")

if __name__ == "__main__":
    asyncio.run(main())


"""
example to start:

$ KAFKA_TOPIC=queueserver python msg_handler.py

"""


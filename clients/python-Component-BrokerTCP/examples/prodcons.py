import Broker

import time

destination = '/python/tests2'
kind = 'QUEUE'

import logging
logging.basicConfig(level=logging.INFO)
#broker logging everything
#logging.getLogger("Broker").setLevel(logging.DEBUG)

from random import random

def sleep(max):
    t = random()*max
    logging.info("Sleeping for %f seconds", t)
    time.sleep(t)


broker = Broker.Client('localhost', 2222)
broker.subscribe(destination, kind)

msg = Broker.Message(payload='este e o payload', destination=destination)

group_id = str(time.time())
id = 0
while True:
    msg.id = "%s, message_id = %d" % (group_id, id)
    id += 1
    broker.produce(msg, kind)
    logging.info("Produced %d ", id)
    sleep(2)
    res = broker.consume()
    logging.info("Consumed %s", res.id)
    #sleep(0.5)
    #broker.acknowledge(res)
    #logging.info("Acknowledged")
    sleep(2)

import logging
#logging.basicConfig(level=logging.INFO)
#broker logging everything
#logging.getLogger("Broker").setLevel(logging.DEBUG)

from time import sleep
from random import random

import Broker

destination = '/python/tests2'

broker = Broker.Client('localhost', 2222)

while True:
    chunk = int(random()*5)+5
    print "Processing %d messages" % chunk
    for _ in xrange(chunk):
        #print "Subscribing"
        broker.request(destination)

    for _ in xrange(chunk):
        msg = broker.consume()
        print msg.timestamp
        print msg.payload
        #logging.info(repr(msg))
        #logging.info("Payload= [%s])", msg.payload)

    s = random()*5 + 1
    print "Sleeping for %f seconds" % s
    sleep(s)

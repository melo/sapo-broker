import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("Broker").setLevel(logging.ERROR)

import Broker
from cPickle import loads

destination = '/logs'
kind = 'TOPIC'

broker = Broker.Client('localhost', 2222)
broker.subscribe(destination, kind)

for id in xrange(10000):
    msg = broker.consume()
    print loads(msg.payload)
    #logging.info(repr(msg))
    #logging.info("Payload= [%s])", msg.payload)

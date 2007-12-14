import Broker

destination = '/python/tests2'
kind = 'QUEUE'

import logging
logging.basicConfig(level=logging.INFO)
#broker logging everything
#logging.getLogger("Broker").setLevel(logging.DEBUG)

broker = Broker.Client('localhost', 2222)
broker.subscribe(destination, kind)

for id in xrange(1000):
    msg = broker.consume()
    logging.info(repr(msg))
    logging.info("Payload= [%s])", msg.payload)

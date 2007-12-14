import Broker

from time import sleep, time

destination = '/python/tests2'
kind = 'QUEUE'

#broker logging everything
#import logging
#logging.basicConfig()
#logging.getLogger("Broker").setLevel(logging.DEBUG)

from random import random

broker = Broker.Client('localhost', 2222)

broker.subscribe(destination, kind)

msg = Broker.Message(payload='este e o payload', destination=destination)

for id in xrange(20000):
    msg.id = "%d, message_id = %d" % (time(), id)
    broker.produce(msg, kind)

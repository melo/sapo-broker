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

for id in xrange(1000):
    msg = broker.consume()
    print msg

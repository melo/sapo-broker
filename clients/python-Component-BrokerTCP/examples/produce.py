# vim: set fileencoding=utf-8 :

import Broker

from time import sleep, time

destination = '/python/tests2'
kind = 'QUEUE'

#broker logging everything
import logging
logging.basicConfig(level=logging.INFO)
#logging.getLogger("Broker").setLevel(logging.DEBUG)

from random import random

broker = Broker.Client('localhost', 2222)

msg = Broker.Message(payload='este é o payload', destination=destination)

for id in xrange(1000):
    msg.id = "%d, message_id = %d" % (time(), id)
    broker.produce(msg, kind)

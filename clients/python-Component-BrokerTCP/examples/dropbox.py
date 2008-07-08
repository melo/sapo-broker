# -*- coding: utf-8 -*-

import Broker

destination = '/python/tests2/dropbox'
kind = 'QUEUE'

#broker logging everything
import logging
#logging.basicConfig(level=logging.INFO)
#logging.getLogger("Broker").setLevel(logging.DEBUG)

from random import random
from time import time

broker = Broker.DropBox('dropbox')

msg = Broker.Message(payload='este é o payload', destination=destination)

for id in xrange(100):
    msg.id = "%d, message_id = %d" % (time(), id)
    broker.produce(msg, kind)

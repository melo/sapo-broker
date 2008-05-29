# -*- coding: utf-8 -*-

import Broker
from time import time

destination = '/python/tests2'
kind = 'QUEUE'

broker = Broker.Client('localhost', 3322)

msg = Broker.Message(payload='este é o payload', destination=destination)

def produce(n):
    for id in xrange(n):
        broker.produce(msg, kind)

while True:
    n = 1000
    t = time()
    produce(n)
    d = time()-t
    print "produced %f msg/s" % (n/d)

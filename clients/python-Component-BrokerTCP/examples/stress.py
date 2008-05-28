from optparse import OptionParser

parser = OptionParser("stresstest for broker (client and server)")
parser.add_option("--server_host", action="store", help="broker address", dest="server_host")
parser.add_option("--server_port", action="store", help="broker port", dest="server_port", type="int")
parser.add_option("--destination", action="store", help="destnation name", dest="destination", type="string", default="/python/stress")
parser.add_option("--producers", action="store", help="number of producers", dest="producers", type="int", default=1)
parser.add_option("--consumers", action="store", help="number of consumers", dest="consumers", type="int", default=1)
parser.add_option("--consumer-messages", action="store", help="number of messages each consumer consumes", dest="consumers_n", type="int", default=10)
parser.add_option("--producer-messages", action="store", help="number of messages each producer produces", dest="producers_n", type="int", default=10)
parser.add_option("--reconnect", action="store", help="maximum number of seconds betwenn reconnects (<0 for no reconnects)", dest="reconnect", type="float", default=-1.0)
(options, args) = parser.parse_args()

import Broker
from threading import Thread, Lock
from time import time, sleep
from random import random

destination = options.destination
kind = 'QUEUE'

import logging
logging.basicConfig(level=logging.INFO)
#broker logging everything
logging.getLogger("Broker").setLevel(logging.WARN)

broker = None
mx = Lock()

def reconnect():
    logging.info('Reconnecting')
    mx.acquire()
    try:
        global broker
        if broker is not None:
            broker.close()
        broker = Broker.Client('10.134.132.63', 3322)
    finally:
        mx.release()

class producer(Thread):
    def __init__ (self, name, count):
        Thread.__init__(self, name=name)
        self.count = count

    def run(self):
        logging.info('starting producer %s', self.getName())
        msg = Broker.Message(payload=u'', destination=destination)
        for n in xrange(self.count):
            msg.payload = u'[%s] Cl\u00e1udio %d' % (self.getName(), n)
            try:
                broker.produce(msg, kind)
            except Exception, e:
                logging.exception(e)
                sleep(1.0)
                continue
        

class consumer(Thread):
    def __init__ (self, name, count):
        Thread.__init__(self, name=name)
        self.count = count

    def run(self):
        logging.info('starting consumer %s', self.getName())
        for n in xrange(self.count):
            try:
                msg = broker.consume()
            except Exception, e:
                logging.exception(e)
                sleep(1.0)
                continue
            logging.info("[%s] got message [%s]", self.getName(), msg.payload)

class recon(Thread):
    def __init__ (self, reconnect):
        Thread.__init__(self, name="reconnect")
        self.__reconnect = reconnect

    def run(self):
        if self.__reconnect>0.0:
            while True:
                delta = random()*self.__reconnect
                logging.info('reconnect in %f s', delta)
                sleep(delta)
                reconnect()
        else:
            return


reconnect()
driver = recon(options.reconnect)
driver.start()

producers = []
consumers = []

for n in range(options.consumers):
    cons = consumer('consumer %d' % n, options.consumers_n)
    consumers.append(cons)
    cons.start()

if options.consumers>0:
    broker.subscribe(destination, kind)

for n in range(options.producers):
    prod = producer('producer %d' % n, options.producers_n)
    producers.append(prod)
    prod.start()

for p in producers:
    logging.info('Waiting for producer %s', p)
    p.join()

for c in consumers:
    logging.info('Waiting for consumer %s', c)
    c.join()

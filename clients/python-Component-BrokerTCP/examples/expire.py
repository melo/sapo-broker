import Broker

destination = '/python/tests2'
kind = 'QUEUE'
DELTA = 10

import logging
from datetime import datetime, timedelta
from time import sleep

logging.basicConfig(level=logging.INFO)
#broker logging everything
logging.getLogger("Broker").setLevel(logging.DEBUG)

broker = Broker.Client('localhost', 2222)

now = datetime.utcnow()
delta1 = timedelta(seconds=DELTA)
delta2 = timedelta(days=7)

#msg1 = Broker.Message(payload='This message shouldn\'t be received', destination=destination, expiration='1997-07-16T19:20:30')
#msg2 = Broker.Message(payload='This message should be received', destination=destination, expiration='1997-07-16T19:20:30')
msg1 = Broker.Message(payload='This message shouldn\'t be received', destination=destination, expiration=now+delta1)
msg2 = Broker.Message(payload='This message should be received', destination=destination, expiration=now+delta2)

logging.info("NOW = %s", now)

broker.produce(msg1, kind)
broker.produce(msg2, kind)

logging.info("Sleeping more than %f seconds", DELTA)
sleep(DELTA+1)
broker.subscribe(destination, kind)

for msg in broker:
    print msg

import logging
import logging.config

import Broker

logging.Broker = Broker.LogHandler

logging.config.fileConfig("log.ini")

logging.info('Info %s %d', 'string', 1)
logging.critical('Critical %s %d', 'string', 1)
logging.exception(LookupError('Bogus message'))

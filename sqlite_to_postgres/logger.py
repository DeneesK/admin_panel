import logging
from logging.handlers import RotatingFileHandler


logging.getLogger()
base_format = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
handler = RotatingFileHandler('db_errors.log', maxBytes=2000000, backupCount=2)
handler.setFormatter(base_format)

logger = logging.getLogger()
logger.addHandler(handler)

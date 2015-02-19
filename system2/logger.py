import logging

logger = logging.getLogger('demosystem')
logger.setLevel(logging.DEBUG)

# debug file handler
fh_d = logging.FileHandler('debug.log')
fh_d.setLevel(logging.DEBUG)

# error file handler
fh_e = logging.FileHandler('error.log')
fh_e.setLevel(logging.ERROR)

# console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh_d.setFormatter(formatter)
fh_e.setFormatter(formatter)

# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh_d)
logger.addHandler(fh_e)

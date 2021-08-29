import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(message)s')
file_handler = logging.FileHandler('info.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
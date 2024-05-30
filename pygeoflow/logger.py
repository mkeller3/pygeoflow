"""Geoflow - Logger"""

import logging
import os
import datetime

if not os.path.exists("log"):
    os.makedirs("log")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f'log/{datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.log')
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and set the format for both handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

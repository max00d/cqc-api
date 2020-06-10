
"""
uses location file to itterate through and get all responses
somewhat crude multi-processing - split into number of processes to use (dependant on CPU)
this allows a simple handling of requests to make sure that the IP throttling can be got around
"""

import requests
import time
import pickle


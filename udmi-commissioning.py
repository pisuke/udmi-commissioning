#!/usr/bin/env python3

import BAC0
from pprint import pprint

bacnet = BAC0.lite()

bacnet.discover()

pprint(bacnet.devices)

while True:
    pass
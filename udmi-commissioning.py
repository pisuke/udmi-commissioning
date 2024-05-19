#!/usr/bin/env python3

import BAC0
from pprint import pprint

bacnet = BAC0.lite(ip="192.18.1.180/24")
# bacnet = BAC0.connect(ip="192.18.1.180/24")

bacnet.discover(global_broadcast=True) #networks=['listofnetworks'] limits=(0,4194303),

pprint(bacnet.devices)

while True:
    pass
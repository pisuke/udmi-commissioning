#!/usr/bin/env python3

"""
  bacnet-scan.py

  Executes a BACnet scan and saves the results to a spreadsheet.

  [Usage] 
  python3 bacnet-scan.py [options]
"""

__author__ = "Francesco Anselmo"
__copyright__ = "Copyright 2024"
__credits__ = ["Francesco Anselmo"]
__license__ = "MIT"
__version__ = "0.1"
__maintainer__ = "Francesco Anselmo"
__email__ = "francesco.anselmo@gmail.com"
__status__ = "Dev"

from pprint import pprint
import argparse
import pandas as pd
import BAC0
from tabulate import tabulate
# from pyfiglet import *
# import pyfiglet.fonts

def show_title():
 """Show the program title
 """
#  f1 = Figlet(font='standard')
#  print(f1.renderText('BACnet-scan'))

 title = """
 ____    _    ____            _                           
| __ )  / \  / ___|_ __   ___| |_      ___  ___ __ _ _ __  
|  _ \ / _ \| |   | '_ \ / _ \ __|____/ __|/ __/ _` | '_ \ 
| |_) / ___ \ |___| | | |  __/ ||_____\__ \ (_| (_| | | | |
|____/_/   \_\____|_| |_|\___|\__|    |___/\___\__,_|_| |_|
 """

 print(title)

def create_data(discovered_devices, network):
    devices = {}
    devices_info = {}
    points = {}
    for each in discovered_devices:
        name, vendor, address, device_id = each

        custom_obj_list = None

        devices[name] = BAC0.device(
            address, device_id, network, poll=0, object_list=custom_obj_list
        )

        devices_info[name] = make_device_info(each)

        points[name] = make_points(devices[name], name)
    return (devices, devices_info, points)

def make_device_info(dev):
    lst = {}
    name, vendor, address, device_id = dev
    lst = {
            "device_name": name,
            "device_vendor": vendor,
            "ip_address": address,
            "device_id": device_id
        }
    df = pd.DataFrame.from_dict(lst, orient="index")
    df.index.name = "property"
    df.rename(columns={0: "value"}, inplace=True)
    print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def make_points(dev, dev_name):
    lst = {}
    for each in dev.points:
        lst[each.properties.name] = {
            "device_name": dev_name,
            "value": each.lastValue,
            "units_or_states": each.properties.units_state,
            "description": each.properties.description,
            "object": "{}:{}".format(each.properties.type, each.properties.address),
            "cloud_device_id": "",
            "cloud_point_name": "",
            "cloud_value": "",
            "validation_status": ""
        }
    df = pd.DataFrame.from_dict(lst, orient="index")
    df.to_csv("%s.csv" % dev_name)
    df.index.name = "point_name"
    print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def make_sheet(dfs, sheet_filename):
    # for k, v in dfs.items():
    #     v.to_csv("%s.csv" % k)
    #     print("Exported device %s point list to file %s.csv" % (k, k))
    with pd.ExcelWriter(sheet_filename) as writer:
        for k, v in dfs.items():
            try:
                v.to_excel(writer, sheet_name=k)
            except:
                pass
    print("Devices point lists written to file %s" % sheet_filename)
    

def main():
    show_title()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False, help="increase the verbosity level")
    parser.add_argument("-x", "--export",  default="bacnet-scan.xlsx", help="spreadsheet file name for scan results (optional), \
                        supported extensions are .xlsx and .ods")
    parser.add_argument("-a", "--address", default="", help="IP address of BACnet interface (optional)")

    args = parser.parse_args()

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
    else:
        BAC0.log_level("silence")

    BACNET_IP_ADDRESS = args.address
    SHEET_FILENAME = args.export

    if BACNET_IP_ADDRESS != "":
        bacnet = BAC0.lite(ip=BACNET_IP_ADDRESS)
    else:
        bacnet = BAC0.lite()
    
    discover = bacnet.discover(global_broadcast=True) 

    devices, devices_info, points = create_data(bacnet.devices, network=bacnet)

    # for device in devices:
    #     print(tabulate(devices_info[device], headers='keys', tablefmt='psql'))
    #     print(tabulate(points[device], headers='keys', tablefmt='psql'))
    make_sheet(points, SHEET_FILENAME)

if __name__ == "__main__":
    main()

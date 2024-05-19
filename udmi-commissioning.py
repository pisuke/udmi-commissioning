#!/usr/bin/env python3

"""
  udmi-commissioning.py

  Checks that values from devices in the field (BACnet and Modbus) are matching values of devices in the cloud (GCP PubSub).

  [Usage] 
  python3 udmi-commissioning.py -p PROJECT_ID -s SUBSCRIPTION_ID -i POINTS_LIST_INPUT_FILE [options]
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
from pyfiglet import *

def show_title():
  """Show the program title
  """
  f1 = Figlet(font='standard')
  print(f1.renderText('UDMI'))
  print(f1.renderText('commissioning'))

def create_data(discovered_devices, network):
    devices = {}
    devices_info = {}
    points = {}
    for each in discovered_devices:
        name, vendor, address, device_id = each
        # print(name, vendor, address, device_id)

        # # try excep eventually as we may have some issues with weird devices
        # if "TEC3000" in name:
        #     custom_obj_list = tec_short_point_list()
        # else:
        custom_obj_list = None

        devices[name] = BAC0.device(
            address, device_id, network, poll=0, object_list=custom_obj_list
        )

        devices_info[name] = make_device_info(each)

        # While we are here, make a dataframe with device
        points[name] = make_points(devices[name])
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
    return df

def make_points(dev):
    lst = {}
    for each in dev.points:
        lst[each.properties.name] = {
            "value": each.lastValue,
            "units_or_states": each.properties.units_state,
            "description": each.properties.description,
            "object": "{}:{}".format(each.properties.type, each.properties.address),
        }
    df = pd.DataFrame.from_dict(lst, orient="index")
    return df

def make_excel(dfs, excel_filename):
    with pd.ExcelWriter(excel_filename) as writer:
        for k, v in dfs.items():
            v.to_excel(writer, sheet_name=k)
    print("Devices point lists written to file %s" % excel_filename)

def main():
    show_title()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False, help="increase the verbosity level")
    parser.add_argument("-l", "--lite", action="store_true", default=False, help="run BAC0 in lite mode")
    parser.add_argument("-p", "--project", default="", help="GCP project id (required)")
    parser.add_argument("-s", "--sub", default="", help="GCP PubSub subscription (required)")
    parser.add_argument("-i", "--input", default="", help="input file containing the point list (required)")
    parser.add_argument("-x", "--excel",  default="results.xlsx", help="excel file name for output results (required)")
    parser.add_argument("-a", "--address", default="", help="IP address of BACnet interface (optional)")
    parser.add_argument("-d", "--device",  default="", help="device name or abbreviation (optional, if not specified shows all devices)")
    

    # parser.add_argument("-t", "--timeout", default="60", help="time interval in seconds for which to receive messages (optional, default=60 seconds)")

    args = parser.parse_args()

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
    else:
        BAC0.log_level("silence")

    if args.project!="" and args.sub!="" and args.input!="" and args.excel!="":
        PROJECT_ID = args.project
        SUBSCRIPTION_ID = args.sub
        POINTS_LIST_INPUT_FILE = args.input
        TARGET_DEVICE_ID = args.device
        BACNET_IP_ADDRESS = args.address
        LITE_MODE = args.lite
        EXCEL_FILENAME = args.excel

        if LITE_MODE:
            if BACNET_IP_ADDRESS != "":
                bacnet = BAC0.lite(ip=BACNET_IP_ADDRESS)
            else:
                bacnet = BAC0.lite()
        else:
            if BACNET_IP_ADDRESS != "":
                bacnet = BAC0.connect(ip=BACNET_IP_ADDRESS)
            else:
                bacnet = BAC0.connect()

        discover = bacnet.discover(global_broadcast=True) #networks=['listofnetworks'], limits=(0,4194303)

        # pprint(discover)
        # pprint(bacnet.devices)
        # print("Discovered BACnet devices", bacnet.discoveredDevices)

        devices = []

        if LITE_MODE:
            devices, devices_info, points = create_data(bacnet.devices, network=bacnet)
            # pprint(devices)
            # print(points)
            for device in devices:
                # name, vendor, address, device_id = device
                # print(name, vendor, address, device_id)
                print(tabulate(devices_info[device], headers='keys', tablefmt='psql')) #
                print(tabulate(points[device], headers='keys', tablefmt='psql')) #
            make_excel(points, EXCEL_FILENAME)
            # print(points.to_markdown()) 
            # for key, value in bacnet.discoveredDevices:
            #     print(key, value)
                # d_manufacturer = row['Manufacturer']
                # d_address = row['Address']
                # d_device_id = row[' Device ID'] # yes, there is a whitespace before Device ID
                # print('Connecting device',d_manufacturer,index,d_address,d_device_id)
                # d = BAC0.device(address=d_address,device_id=d_device_id,network=bacnet)
                # devices.append(d)
            # for device in bacnet.devices:
            #     devices.append(device)
            #     print(dir(device))
        else:
            for index, row in bacnet.devices.iterrows():
                d_manufacturer = row['Manufacturer']
                d_address = row['Address']
                d_device_id = row[' Device ID'] # yes, there is a whitespace before Device ID
                print('Connecting device',d_manufacturer,index,d_address,d_device_id)
                d = BAC0.device(address=d_address,device_id=d_device_id,network=bacnet)
                devices.append(d)

        # pprint(devices)

        for device in devices:
            try:
                print(device.bacnet_properties['objectName'])
            except:
                pass
            try:        
                print(device.points)
            except:
                pass
            try:
                print(device.bacnet_properties)
            except:
                pass
            # print(10*'-')

        # while True:
        #     pass

if __name__ == "__main__":
    main()
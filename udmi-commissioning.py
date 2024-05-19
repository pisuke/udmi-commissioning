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

from os.path import exists
from pprint import pprint
import json
import argparse
import pandas as pd
import BAC0
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
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

        custom_obj_list = None

        devices[name] = BAC0.device(
            address, device_id, network, poll=0, object_list=custom_obj_list
        )

        devices_info[name] = make_device_info(each)

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

def make_sheet(dfs, excel_filename):
    with pd.ExcelWriter(excel_filename) as writer:
        for k, v in dfs.items():
            v.to_excel(writer, sheet_name=k)
    print("Devices point lists written to file %s" % excel_filename)

def get_sheet_dict(sheet_file, sheet_name):
    dataframe = pd.read_excel(sheet_file, sheet_name, dtype=str)

    # Replace "nan" values with empty whitespaces
    dataframe = dataframe.fillna("")

    # Remove all trailing whitespaces
    for column in dataframe.columns:
      dataframe[column] = dataframe[column].apply(
        lambda dataframe_column: dataframe_column.strip()
      )

    # return dataframe.to_dict('records')
    return dataframe

def print_message(message):
    body = message.data
    device_id = message.attributes['deviceId']
    gateway_id = message.attributes['gatewayId']
    sub_folder = message.attributes['subFolder']
    type = message.attributes['subType']
    timestamp = json.loads(body)['timestamp']
  
    print(tabulate([["Timestamp", "Device ID", "Gateway ID", "Subfolder", "Type"],
                        [timestamp, device_id, gateway_id, sub_folder, type]]))
    print(message)
    print(body)
    print(70*"-")

    # df.loc[df['column_name'] == some_value]
    

def message_callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global devices_points
    
    body = message.data
    device_id = message.attributes['deviceId']
    gateway_id = message.attributes['gatewayId']
    sub_folder = message.attributes['subFolder']
    type = message.attributes['subType']
    timestamp = json.loads(body)['timestamp']

    if sub_folder == "pointset" and type == "":
        points = json.loads(body)['points']
        # print(dir(points))
        for point in points.items():
            point_name = point[0]
            value = point[1]['present_value']
            print(device_id, point_name, value)
            for device in devices_points:
                device_points = devices_points[device]
                match = device_points.loc[device_points['cloud_point_name'] == point_name]
                if not match.equals(pd.DataFrame.empty):
                    print(match)
                    
    message.ack()

devices_points = {}

def main():
    global devices_points
    show_title()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False, help="increase the verbosity level")
    # parser.add_argument("-l", "--lite", action="store_true", default=False, help="run BAC0 in lite mode")
    parser.add_argument("-p", "--project", default="", help="GCP project id (required)")
    parser.add_argument("-s", "--sub", default="", help="GCP PubSub subscription (required)")
    parser.add_argument("-i", "--input", default="input.xlsx", help="input file containing the point list (optional, \
                        the default is input.xlsc, accepted extensions are .xlsx and .ods)")
    parser.add_argument("-o", "--output",  default="output.xlsx", help="sheet file name for output results (optional, \
                        the default is output.xlsx, accepted extensions are .xlsx and .ods)")
    parser.add_argument("-a", "--address", default="", help="IP address of BACnet interface (optional)")
    parser.add_argument("-t", "--timeout", default="3600", help="time interval in seconds for which to receive messages (optional, \
                        default=3600 seconds, equating to 1 hour)")

    args = parser.parse_args()

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
    else:
        BAC0.log_level("silence")

    if args.project!="" and args.sub!="" and args.input!="" and args.output!="":
        PROJECT_ID = args.project
        SUBSCRIPTION_ID = args.sub
        POINTS_LIST_INPUT_FILE = args.input
        BACNET_IP_ADDRESS = args.address
        # LITE_MODE = args.lite
        LITE_MODE = True
        SHEET_FILENAME = args.output

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

        # discover = bacnet.discover(global_broadcast=True)

        # devices = []

        if exists(POINTS_LIST_INPUT_FILE):
            spreadsheet = pd.ExcelFile(POINTS_LIST_INPUT_FILE)
            for sheet_name in spreadsheet.sheet_names:
                print(sheet_name)
                devices_points[sheet_name] = get_sheet_dict(POINTS_LIST_INPUT_FILE, sheet_name)
                pprint(devices_points[sheet_name])
                print(tabulate(devices_points[sheet_name], headers='keys', tablefmt='psql'))

        # Number of seconds the subscriber should listen for messages
        TIMEOUT = int(args.timeout)

        subscriber = pubsub_v1.SubscriberClient()
        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_id}`
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=message_callback)
        
        print(f"Listening for messages from all devices on {subscription_path}\n")

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=TIMEOUT)
            except TimeoutError:
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.

        # pprint(devices_points)

        # if LITE_MODE:
        #     devices, devices_info, points = create_data(bacnet.devices, network=bacnet)
        #     # pprint(devices)
        #     # print(points)
        #     for device in devices:
        #         # name, vendor, address, device_id = device
        #         # print(name, vendor, address, device_id)
        #         print(tabulate(devices_info[device], headers='keys', tablefmt='psql')) #
        #         print(tabulate(points[device], headers='keys', tablefmt='psql')) #
        #     make_sheet(points, SHEET_FILENAME)
        #     # print(points.to_markdown()) 
        #     # for key, value in bacnet.discoveredDevices:
        #     #     print(key, value)
        #         # d_manufacturer = row['Manufacturer']
        #         # d_address = row['Address']
        #         # d_device_id = row[' Device ID'] # yes, there is a whitespace before Device ID
        #         # print('Connecting device',d_manufacturer,index,d_address,d_device_id)
        #         # d = BAC0.device(address=d_address,device_id=d_device_id,network=bacnet)
        #         # devices.append(d)
        #     # for device in bacnet.devices:
        #     #     devices.append(device)
        #     #     print(dir(device))
        # else:
        #     for index, row in bacnet.devices.iterrows():
        #         d_manufacturer = row['Manufacturer']
        #         d_address = row['Address']
        #         d_device_id = row[' Device ID'] # yes, there is a whitespace before Device ID
        #         print('Connecting device',d_manufacturer,index,d_address,d_device_id)
        #         d = BAC0.device(address=d_address,device_id=d_device_id,network=bacnet)
        #         devices.append(d)

        # # pprint(devices)

        # for device in devices:
        #     try:
        #         print(device.bacnet_properties['objectName'])
        #     except:
        #         pass
        #     try:        
        #         print(device.points)
        #     except:
        #         pass
        #     try:
        #         print(device.bacnet_properties)
        #     except:
        #         pass
        #     # print(10*'-')

if __name__ == "__main__":
    main()
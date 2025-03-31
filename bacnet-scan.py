#!/usr/bin/env python3

"""
  bacnet-scan.py

  Executes a BACnet scan and saves the results to a spreadsheet.

  [Usage] 
  python3 bacnet-scan.py [options]
"""

__author__ = "Francesco Anselmo"
__copyright__ = "Copyright 2025"
__credits__ = ["Francesco Anselmo"]
__license__ = "MIT"
__version__ = "0.3"
__maintainer__ = "Francesco Anselmo"
__email__ = "francesco.anselmo@gmail.com"
__status__ = "Dev"

from pprint import pprint
import argparse
import pandas as pd
import BAC0
from tabulate import tabulate
import os

def show_title():
    """Show the program title
    """

    title = """
    ____    _    ____            _                           
    | __ )  / \  / ___|_ __   ___| |_      ___  ___ __ _ _ __  
    |  _ \ / _ \| |   | '_ \ / _ \ __|____/ __|/ __/ _` | '_ \ 
    | |_) / ___ \ |___| | | |  __/ ||_____\__ \ (_| (_| | | | |
    |____/_/   \_\____|_| |_|\___|\__|    |___/\___\__,_|_| |_|
    """

    print(title)

def create_data(output_path, verbose, discovered_devices, network, devicesonly):
    devices = {}
    devices_info = {}
    points = {}
    for each in discovered_devices:
        name, vendor, address, device_id = each

        custom_obj_list = None

        devices[name] = BAC0.device(
            address, device_id, network, poll=0, object_list=custom_obj_list
        )

        devices_info[name] = make_device_info(output_path, verbose, each, network)

        if not devicesonly:
            points[name] = make_points(output_path, verbose, devices[name], name)
    return (devices, devices_info, points)

def make_device_info(output_path, verbose, dev, network):
    lst = {}
    
    name, vendor, address, device_id = dev

    device = BAC0.device(
            address, device_id, network, poll=0
        )
    
    print("device bacnet properties:", device.bacnet_properties)
    
    try:
        print("device address binding:", device.deviceAddressBinding)
        print(dir(device.deviceAddressBinding))
    except:
        pass
    
    try:
        print("network: ", network)
        print(dir(network))
    except:
        pass
    
    try:
        print("device:", device)
        print("device properties:", device.properties)
        print(dir(device))
    except:
        pass
    
    try:
        description = device.bacnet_properties["description"]
    except:
        description = ""
        
    try:
        location = device.bacnet_properties["location"]
    except:
        location = ""

    try:
        application_software_version = device.bacnet_properties["applicationSoftwareVersion"]
    except:
        application_software_version = ""
    
    try:
        firmware_revision = device.bacnet_properties["firmwareRevision"]
    except:
        firmware_revision = ""
    
    try:
        vendor_name = device.bacnet_properties["vendorName"]
    except:
        vendor_name = ""

    try:
        model_name = device.bacnet_properties["modelName"]
    except:
        model_name = ""

    try:
        serial_number = device.bacnet_properties["serialNumber"]
    except:
        serial_number = ""
        
    try:
        network_number = device.bacnet_properties["networkNumber"]
    except:
        network_number = ""

    lst = {
            "device_name": name,
            "device_vendor": vendor_name,
            "device_model": model_name,
            "device_firmware": firmware_revision,
            "description": description,
            "location": location,
            "device_application_version": application_software_version,
            "device_serial_number": serial_number,
            "ip_address": address,
            "device_id": device_id,
            "network": network_number
        }
    df = pd.DataFrame.from_dict(lst, orient="index")
    df.index.name = "property"
    df.rename(columns={0: "value"}, inplace=True)
    if verbose:
        print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def make_points(output_path, verbose, dev, dev_name):
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
    df.index.name = "point_name"
    df.to_csv(os.path.join(output_path, "%s.csv" % dev_name))
    if verbose:
        print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def make_sheet(devices_df, dfs, sheet_filename):
    with pd.ExcelWriter(sheet_filename) as writer:
        devices_df.to_excel(writer, sheet_name="devices")
        for k, v in dfs.items():
            try:
                v.to_excel(writer, sheet_name=k)
            except:
                pass
    print("Devices point lists written to file %s" % sheet_filename)
    

def string_to_integer_list(comma_separated_string):
  """Converts a comma-separated string of numbers to a list of integers.

  Args:
    comma_separated_string: A string containing numbers separated by commas.

  Returns:
    A list of integers. Returns an empty list if the input string is empty
    or contains only commas. Raises a ValueError if any element cannot be
    converted to an integer.
  """
  if not comma_separated_string.strip():
    return []

  string_numbers = comma_separated_string.split(',')
  integer_list = []
  for s in string_numbers:
    try:
      number = int(s.strip())  # Remove leading/trailing whitespace and convert to integer
      integer_list.append(number)
    except ValueError:
      raise ValueError(f"Invalid integer format: '{s.strip()}'")
  return integer_list
    
def main():
    show_title()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False, help="increase the verbosity level (optional)")
    parser.add_argument("-x", "--export",  default="bacnet-scan.xlsx", help="spreadsheet file name for scan results (optional), \
                        supported extensions are .xlsx and .ods")
    parser.add_argument("-a", "--address", default="", help="IP address of BACnet interface (optional)")
    parser.add_argument("-n", "--networks", default="", help="comma separated target list of BACnet networks (optional)")
    parser.add_argument("-b", "--bacnetid", default="", help="restrict the enumeration to only one device with this BACnet ID (optional)")
    parser.add_argument("-d", "--deviceonly", action="store_true", default=False, help="only execute a BACnet WHOIS device scan with no point enumeration (optional)")

    args = parser.parse_args()

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
    else:
        BAC0.log_level("silence")

    BACNET_IP_ADDRESS = args.address
    SHEET_FILENAME = args.export
    BACNET_NETWORKS = args.networks
    BACNET_DEVICE_ID = args.bacnetid
    DEVICE_ONLY_SCAN = args.deviceonly

    if BACNET_IP_ADDRESS != "":        
        bacnet = BAC0.lite(ip=BACNET_IP_ADDRESS)
    else:
        bacnet = BAC0.lite()
    
    if BACNET_NETWORKS == "":
        if BACNET_DEVICE_ID == "":
            discover = bacnet.discover(global_broadcast=True) 
        else:
            discover = bacnet.discover(global_broadcast=True, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID))
    else:
        bacnet_networks = string_to_integer_list(BACNET_NETWORKS)
        if BACNET_DEVICE_ID == "":
            discover = bacnet.discover(global_broadcast=True, networks=bacnet_networks) 
        else:
            BACNET_DEVICE_ID = int(BACNET_DEVICE_ID)
            discover = bacnet.discover(global_broadcast=True, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID), networks=bacnet_networks)

    output_path = "bacnet_devices"

    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    devices_df = pd.DataFrame()
    for device in bacnet.devices:
        devices_df = pd.concat( [devices_df, make_device_info(output_path, args.verbose, device, network=bacnet)], ignore_index=True, axis=1)
    devices_df = devices_df.transpose()
    devices_df.index.name = "number"
    print(tabulate(devices_df, headers='keys', tablefmt='psql'))
    devices_df.to_csv(os.path.join(output_path, "%s.csv" % "bacnet_devices_list"))

    devices, devices_info, points = create_data(output_path, args.verbose, bacnet.devices, network=bacnet, devicesonly=DEVICE_ONLY_SCAN)

    make_sheet(devices_df, points, os.path.join(output_path, SHEET_FILENAME))

if __name__ == "__main__":
    main()

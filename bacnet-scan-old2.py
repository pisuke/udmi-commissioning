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
import re
import sys
# import ipaddress
import logging
import time
from pprint import pprint

def show_title():
    """Show the program title
    """

    title = r""" 
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
        
        sanitized_dev_name = sanitize_device_name(name)

        devices[sanitized_dev_name] = BAC0.device(
            address, device_id, network, poll=0, object_list=custom_obj_list
        )

        devices_info[sanitized_dev_name] = make_device_info(output_path, verbose, each, network)

        if not devicesonly:
            points[sanitized_dev_name] = make_points(output_path, verbose, devices[sanitized_dev_name], sanitized_dev_name)
    return (devices, devices_info, points)

def make_device_info_simple(output_path, verbose, dev, network):
    lst = {}
    
    address, device_id = dev

    #device = BAC0.device(
    #        address, device_id, network, poll=0
    #    )
    
    #try:
    #    ipaddress.ip_address(address)
    #except ValueError:
    #    pass
    
    try:
        object_name, vendor_name, firmware_version, model_name, serial_number, description, location, application_software_version = (
            network.readMultiple(
                f"{address} device {device_id} objectName vendorName"
                " firmwareRevision modelName serialNumber description location applicationSoftwareVersion"
            )
        )

        logging.info("object_name: %s vendor_name: %s firmware: %s model: %s serial: %s description: %s location: %s",  
                      object_name, vendor_name, firmware_version, model_name, serial_number, description, location, application_software_version)

    except (BAC0.core.io.IOExceptions.SegmentationNotSupported, Exception) as err:
        logging.exception(f"error reading from {address}/{device_id}")        
    
    sanitized_dev_name = sanitize_device_name(object_name)
    
    lst = {
            "device_name": object_name,
            "sanitized_device_name": sanitized_dev_name,
            "device_vendor": vendor_name,
            "device_model": model_name,
            "device_firmware": firmware_version,
            "description": description,
            "location": location,
            "device_application_version": application_software_version,
            "device_serial_number": serial_number,
            "ip_address": address,
            "device_id": device_id
            # "network": network_number
        }
    df = pd.DataFrame.from_dict(lst, orient="index")
    df.index.name = "property"
    df.rename(columns={0: "value"}, inplace=True)
    
    if verbose:
        print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def make_device_info(output_path, verbose, dev, network):
    lst = {}
    
    name, vendor, address, device_id = dev
    
    sanitized_dev_name = sanitize_device_name(name)

    device = BAC0.device(
            address, device_id, network, poll=0
        )
    
    # print("device bacnet properties:", device.bacnet_properties)
    
    # try:
    #     print("device address binding:", device.deviceAddressBinding)
    #     print(dir(device.deviceAddressBinding))
    # except:
    #     pass
    
    # try:
    #     print("network: ", device.properties.network)
    #     print(dir(device.properties.network))
    #     print(device.properties.network.networkNumber)
    #     print(device.properties.network.what_is_network_number)
    #     print(dir(device.properties.network.what_is_network_number))
    # except:
    #     pass
    
    # try:
    #     print("device:", device)
    #     print("device properties:", device.properties)
    #     print(dir(device))
    # except:
    #     pass
    
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
            "sanitized_device_name": sanitized_dev_name,
            "device_vendor": vendor_name,
            "device_model": model_name,
            "device_firmware": firmware_revision,
            "description": description,
            "location": location,
            "device_application_version": application_software_version,
            "device_serial_number": serial_number,
            "ip_address": address,
            "device_id": device_id
            # "network": network_number
        }
    df = pd.DataFrame.from_dict(lst, orient="index")
    df.index.name = "property"
    df.rename(columns={0: "value"}, inplace=True)
    if verbose:
        print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def make_points(output_path, verbose, dev, dev_name):
    lst = {}
    sanitized_dev_name = sanitize_device_name(dev_name)
    for each in dev.points:
        lst[each.properties.name] = {
            "device_name": dev_name,
            "sanitized_device_name": sanitized_dev_name,
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
    
    df.to_csv(os.path.join(output_path, "%s.csv" % sanitized_dev_name))
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

def sanitize_unix_command(input_string):
    """
    Sanitizes a string for use in a Unix command line by removing or replacing
    characters that could cause issues.

    Args:
        input_string: The string to sanitize.

    Returns:
        A sanitized string suitable for use in a Unix command line.
    """
    # Characters to remove or replace for Unix command line safety
    # These are often used for special purposes by the shell
    offending_unix_chars = r"[;&|<>`'$(){}\[\]#\s:/]"

    # Replace offending characters with underscores (or you could remove them)
    sanitized_string = ""
    try:
        sanitized_string = re.sub(offending_unix_chars, "_", input_string)
    except:
        pass

    return sanitized_string  

def sanitize_spreadsheet_tabs(input_string):
    """
    Sanitizes a string for use in a spreadsheet by removing or replacing
    tab characters that would create new columns.

    Args:
        input_string: The string to sanitize.

    Returns:
        A sanitized string suitable for use within a single spreadsheet cell.
    """
    # Replace tab characters (\t) with a space or another suitable character
    sanitized_string = input_string.replace('\t', '_')
    return sanitized_string  

def sanitize_device_name(input_string):
    """
    Sanitizes a string for both Unix command line (including colons) and
    spreadsheet tabs.

    Args:
        input_string: The string to sanitize.

    Returns:
        A string sanitized for both contexts.
    """
    sanitized_unix = sanitize_unix_command(input_string)
    sanitized_both = sanitize_spreadsheet_tabs(sanitized_unix)
    return sanitized_both

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
    parser.add_argument("-b", "--bacnetid", default="", help="restrict the scan to only one device with this BACnet ID (optional)")
    parser.add_argument("-r", "--range", default="",  help="restrict the scan to a device range in the format BACnet_ID_start,BACnet_ID_finish as in 1234,5678 (optional)")
    parser.add_argument("-d", "--deviceonly", action="store_true", default=False, help="only execute a BACnet WHOIS device scan with no point enumeration (optional)")
    parser.add_argument("-g", "--globalscan", action="store_true", default=False, help="execute a global broadcast BACnet scan (optional)")

    args = parser.parse_args()

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
    else:
        BAC0.log_level("silence")

    BACNET_IP_ADDRESS = args.address
    SHEET_FILENAME = args.export
    SHEET_FILENAME_NAME, SHEET_FILENAME_EXT = os.path.splitext(SHEET_FILENAME)
    BACNET_NETWORKS = args.networks
    BACNET_DEVICE_ID = args.bacnetid
    BACNET_RANGE = args.range
    BACNET_GLOBAL_SCAN = args.globalscan
    DEVICE_ONLY_SCAN = args.deviceonly
    
    BACNET_RANGE_START = 0
    BACNET_RANGE_FINISH = 4194302
    
    print("Bacnet Global Scan:", BACNET_GLOBAL_SCAN)

    if BACNET_IP_ADDRESS != "":        
        bacnet = BAC0.lite(ip=BACNET_IP_ADDRESS)
    else:
        bacnet = BAC0.lite()
    
    if BACNET_NETWORKS == "":
        if BACNET_DEVICE_ID == "":
            if BACNET_RANGE != "":
                BACNET_RANGE_START = BACNET_RANGE.split(",")[0]
                BACNET_RANGE_FINISH = BACNET_RANGE.split(",")[1]
                print("start:", BACNET_RANGE_START)
                print("finish:", BACNET_RANGE_FINISH)
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_RANGE_START,BACNET_RANGE_FINISH))
            else:
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN) 
        else:
            discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID))
    else:
        bacnet_networks = string_to_integer_list(BACNET_NETWORKS)
        if BACNET_DEVICE_ID == "":
            if BACNET_RANGE != "":
                BACNET_RANGE_START = BACNET_RANGE.split(",")[0]
                BACNET_RANGE_FINISH = BACNET_RANGE.split(",")[1]
                print("start:", BACNET_RANGE_START)
                print("finish:", BACNET_RANGE_FINISH)
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_RANGE_START,BACNET_RANGE_FINISH), networks=bacnet_networks)
            else:
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, networks=bacnet_networks) 
        elif BACNET_DEVICE_ID != "":
            BACNET_DEVICE_ID = int(BACNET_DEVICE_ID)
            discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID), networks=bacnet_networks)

    output_path = "bacnet_devices"

    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    #devices_df = pd.DataFrame()
    
    if DEVICE_ONLY_SCAN:
        discovered_devices = (
                  set(bacnet.discoveredDevices.keys())
              )
              
        #pprint(bacnet.devices)
        
        devices_df = pd.DataFrame()
        
        for device in discovered_devices:
            # print(dir(device))
            address = device[0]
            device_id = device[1]
            print(address, device_id)
            devices_df = pd.concat( [devices_df, make_device_info_simple(output_path, args.verbose, device, network=bacnet)], ignore_index=True, axis=1)
            
        pprint(devices_df)
    
        devices_df = devices_df.transpose()
        devices_df.index.name = "number"
        print(tabulate(devices_df, headers='keys', tablefmt='psql'))
        devices_df.to_csv(os.path.join(output_path, "%s.csv" % SHEET_FILENAME_NAME))

        #bacnet_devices_df = pd.DataFrame(bacnet.devices, columns=['device_name', 'manufacturer', 'address', 'device_id'])
        #bacnet_devices_df.index.name = "number"
        #bacnet_devices_df.to_csv(os.path.join(output_path, "%s_devicelist.csv" % SHEET_FILENAME_NAME))
    
        #pprint(bacnet_devices_df)
    
    #devices = []
    #for d_name, d_manufacturer, d_address, d_device_id in bacnet.devices:
    #    print('Connecting device',d_manufacturer,d_address,d_device_id)
    #    custom_obj_list = None 
    #    d = BAC0.device(
    #        d_address, d_device_id, bacnet, poll=0, object_list=custom_obj_list
    #    )
    #    #d = BAC0.device(address=d_address,device_id=d_device_id,network=bacnet)
    #    devices.append(d)
    #pprint(devices)
    
    #for device in discovered_devices:
    #   address = device[0]
    #    device_id = device[1]
    #    object_name = ""
    #    object_list = []
    #    try:
    #       object_name, object_list = (
    #           bacnet.readMultiple(
    #               f"{address} device {device_id} objectName objectList "
    #            )
    #       )
    #    except:
    #       pass
    #    print(object_name, object_list)
    
    if not DEVICE_ONLY_SCAN:
        
        #for device in discovered_devices:
        #    # print(dir(device))
        #    address = device[0]
        #    device_id = device[1]
        #    print(address, device_id)
        #    devices_df = pd.concat( [devices_df, make_device_info_simple(output_path, args.verbose, device, network=bacnet)], ignore_index=True, axis=1)
        
        #pprint(devices_df)
    
        #devices_df = devices_df.transpose()
        #devices_df.index.name = "number"
        #print(tabulate(devices_df, headers='keys', tablefmt='psql'))
        #devices_df.to_csv(os.path.join(output_path, "%s.csv" % SHEET_FILENAME_NAME))

        devices, devices_info, points = create_data(output_path, args.verbose, bacnet.devices, network=bacnet, devicesonly=DEVICE_ONLY_SCAN)
        
        #print(tabulate(devices_info, headers='keys', tablefmt='psql'))
        devices_df = pd.DataFrame()
        #for index, row in devices_info.iterrows():
        for key, value in devices_info.items():
            devices_df = pd.concat( [devices_df, value], ignore_index=True, axis=1)
        #devices_df = pd.DataFrame.from_dict(devices_info, orient='index')
        devices_df = devices_df.transpose()
        devices_df.index.name = "number"
        print(tabulate(devices_df, headers='keys', tablefmt='psql'))

        make_sheet(devices_df, points, os.path.join(output_path, SHEET_FILENAME))

if __name__ == "__main__":
    main()
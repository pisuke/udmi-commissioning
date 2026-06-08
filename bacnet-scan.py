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
__version__ = "0.41"
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
# import logging
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

# -- Direct Device Discovery Function --
def find_single_device(bacnet, device_ip):
    """
    Finds a single BACnet device at a known IP address.
    Returns a list containing one device tuple: [(address, instance_id)].
    """
    # logging.info(f"Sending targeted Who-Is to {device_ip}...")
    print(f"Sending targeted Who-Is to {device_ip}...")
    # print(help(bacnet.whois_router_to_network))
    bacnet.whois_router_to_network(network=None, destination=device_ip)
    bacnet.whois(device_ip, global_broadcast=True)
    # A short, fixed wait is acceptable here as we're targeting one known device
    time.sleep(2)

    # pprint(bacnet.devices)

    # try:
    #     for address, instance_id in bacnet.devices:
    #         # The address in bacnet.devices may include the port (e.g., '10.0.0.5:47808')
    #         if address.startswith(device_ip):
    #             # logging.info(f"Device responded. Address: {address}, Instance ID: {instance_id}")
    #             print(f"Device responded. Address: {address}, Instance ID: {instance_id}")
    #             return [(address, instance_id)]
    # except:
    #     pass

    # try:
    for name, manufacturer, address, instance_id in bacnet.devices:
        # The address in bacnet.devices may include the port (e.g., '10.0.0.5:47808')
        if address.startswith(device_ip):
            # logging.info(f"Device responded. Address: {address}, Instance ID: {instance_id}")
            print(f"Device responded. Address: {address}, Instance ID: {instance_id}")
            return [(name, manufacturer, address, instance_id)]
    # except:
    #     pass
    
    return []

# -- Device Discovery Function --
def discover_devices(bacnet, subnet_broadcast, interval, checks):
    """
    Discovers BACnet devices on a network using a stable discovery method.

    Args:
        bacnet (BAC0.lite): An initialized BAC0 client instance.
        subnet_broadcast (str): The broadcast address of the target subnet.
        interval (int): Polling interval for stability checks.
        checks (int): Number of stable checks to wait for.

    Returns:
        list: A list of discovered device tuples (address, instance_id).
    """
    # logging.info(f"Sending Who-Is to {subnet_broadcast} and entering stable discovery loop...")
    print(f"Sending Who-Is to {subnet_broadcast} and entering stable discovery loop...")
    bacnet.whois_router_to_network(network=None, destination=subnet_broadcast)
    bacnet.whois(subnet_broadcast, global_broadcast=True)

    last_device_count = 0
    stability_counter = 0
    while stability_counter < checks:
        # logging.info(f"Waiting {interval}s... (Stability Check: {stability_counter + 1}/{checks})")
        # print(f"Waiting {interval}s... (Stability Check: {stability_counter + 1}/{checks})")
        time.sleep(interval)
        
        current_device_count = len(bacnet.devices)
        if current_device_count > last_device_count:
            # logging.info(f"Found new device(s). Total now: {current_device_count}. Resetting counter.")
            # print(f"Found new device(s). Total now: {current_device_count}. Resetting counter.")
            print(f"Found new device(s). Total now: {current_device_count}.")
            stability_counter = 0
        else:
            stability_counter += 1
            # logging.info(f"No new devices. Stability count: {stability_counter}.")
            # print(f"No new devices. Stability count: {stability_counter}.")
        last_device_count = current_device_count
    
    # logging.info("Discovery stable.")
    print("BACnet discovery completed.")
    return bacnet.devices
    # return (set(bacnet.discoveredDevices.keys()))

# -- Point Enumeration Function --
def enumerate_device_points(bacnet, discovered_devices):
    """
    Iterates through discovered devices to read their properties and enumerate their objects.
    Returns a tuple: (devices_df, objects_by_device_dict).
    """
    device_properties = ['objectName', 'vendorName', 'modelName', 'serialNumber', 'description']
    object_properties = ['objectName', 'description', 'presentValue', 'units']
    
    all_devices_data = []
    objects_by_device = {}

    logging.info(f"Found {len(discovered_devices)} device(s) to process. Starting enumeration...")

    for address, instance_id in discovered_devices:
        logging.info(f"--- Processing Device at {address} (Instance: {instance_id}) ---")
        
        device_info = {'deviceAddress': address, 'deviceInstance': instance_id}
        try:
            props = bacnet.readMultiple(f'{address} device {instance_id}', device_properties)
            device_info['deviceName'] = props.get('objectName')
            device_info.update({k: v for k, v in props.items() if k != 'objectName'})
            all_devices_data.append(device_info)
        except Exception as e:
            logging.error(f"  - Could not read properties from device {instance_id}, skipping. Error: {e}")
            continue

        device_objects_data = []
        try:
            object_list = bacnet.read(f'{address} device {instance_id} objectList')
            if not isinstance(object_list, list) or not object_list:
                logging.warning(f"  - No objects found for device {instance_id}.")
                objects_by_device[instance_id] = pd.DataFrame()
                continue
        except Exception as e:
            logging.error(f"  - Could not read object list from device {instance_id}. Error: {e}")
            continue

        logging.info(f"  Enumerating {len(object_list)} objects...")
        for obj_type, obj_instance in object_list:
            if not isinstance(obj_type, str): continue
            
            row_data = {'objectType': obj_type, 'objectInstance': obj_instance}
            try:
                obj_props = bacnet.readMultiple(f'{address} {obj_type} {obj_instance}', object_properties)
                row_data.update(obj_props)
            except Exception as e:
                logging.warning(f"    - Could not read properties for {obj_type} {obj_instance}: {e}")
            
            device_objects_data.append(row_data)

        objects_by_device[instance_id] = pd.DataFrame(device_objects_data)

    return pd.DataFrame(all_devices_data), objects_by_device


def create_data(output_path, verbose, discovered_devices, network, devicesonly):
    devices = {}
    points = {}
    
    for each in discovered_devices:
        name, vendor, address, device_id = each
        sanitized_dev_name = sanitize_device_name(name)

        try:
            devices[sanitized_dev_name] = BAC0.device(
                address, device_id, network, poll=0, object_list=None
            )
            
            if not devicesonly:
                points[sanitized_dev_name] = make_points(output_path, verbose, devices[sanitized_dev_name], sanitized_dev_name)
        
        except Exception as e:
            print(f"Failed to initialize/enumerate device {name} ({address}): {e}")
            continue
            
    return (devices, points)

def make_device_info_simple(output_path, verbose, dev, network):
    data = {
        "device_name": "N/A", "device_vendor": "N/A", "device_model": "N/A",
        "device_firmware": "N/A", "description": "N/A", "location": "N/A",
        "device_application_version": "N/A", "device_serial_number": "N/A",
        "ip_address": dev[2], "device_id": dev[3]
    }
    
    try:
        props = network.readMultiple(
            f"{dev[2]} device {dev[3]} objectName vendorName "
            "firmwareRevision modelName serialNumber description location applicationSoftwareVersion"
        )
        data.update({
            "device_name": props.get('objectName', 'N/A'),
            "device_vendor": props.get('vendorName', 'N/A'),
        })
    except Exception as e:
        print(f"Skipping detailed read for {dev[2]} due to: {e}")

    df = pd.DataFrame.from_dict(data, orient="index")
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
    parser.add_argument("-a", "--address", default="", help="IP address of BACnet interface in Mango (optional)")
    parser.add_argument("-n", "--networks", default="", help="comma separated target list of BACnet networks (optional)")
    parser.add_argument("-b", "--bacnetid", default="", help="restrict the scan to only one device with this BACnet ID (optional)")
    parser.add_argument("-r", "--range", default="",  help="restrict the scan to a device range in the format BACnet_ID_start,BACnet_ID_finish as in 1234,5678 (optional)")
    parser.add_argument("-d", "--deviceonly", action="store_true", default=False, help="only execute a BACnet WHOIS device scan with no point enumeration (optional)")
    parser.add_argument("-g", "--globalscan", action="store_true", default=False, help="execute a global broadcast BACnet scan (optional)")
    parser.add_argument("-s", "--subnet_broadcast", default="", help="restrict the scan to a specific subnet broadcast address (optional)")
    parser.add_argument("-i", "--ip", default="", help="restrict the scan to a specific device with this IP address (optional)")

    args = parser.parse_args()

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
        # log_level = logging.DEBUG
    else:
        BAC0.log_level("silence")
        # log_level = logging.INFO
        pass

    # logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    BACNET_IP_ADDRESS = args.address
    SHEET_FILENAME = args.export
    SHEET_FILENAME_NAME, SHEET_FILENAME_EXT = os.path.splitext(SHEET_FILENAME)
    BACNET_NETWORKS = args.networks
    BACNET_DEVICE_ID = args.bacnetid
    BACNET_RANGE = args.range
    BACNET_GLOBAL_SCAN = args.globalscan
    DEVICE_ONLY_SCAN = args.deviceonly
    TARGET_SUBNET_BROADCAST = args.subnet_broadcast 
    TARGET_IP_ADDRESS = args.ip
    
    BACNET_RANGE_START = 0
    BACNET_RANGE_FINISH = 4194302
    # -- Reliable Discovery Settings --
    # How often to check for new devices (in seconds)
    POLLING_INTERVAL = 5
    # How many consecutive checks with no new devices before we assume discovery is complete
    STABILITY_CHECKS = 3

    
    # logging.info(("Bacnet Global Scan:", BACNET_GLOBAL_SCAN))
    print(("Bacnet Global Scan:", BACNET_GLOBAL_SCAN))

    # logging.info("Initializing BAC0 client...")
    print("Initializing BAC0 client...")
    if BACNET_IP_ADDRESS != "":        
        bacnet = BAC0.lite(ip=BACNET_IP_ADDRESS)
    else:
        bacnet = BAC0.lite()

    # Step 1: Discover Devices
    
    if TARGET_SUBNET_BROADCAST != "":
        discovered_devices = discover_devices(bacnet, TARGET_SUBNET_BROADCAST, POLLING_INTERVAL, STABILITY_CHECKS)        

    elif TARGET_IP_ADDRESS != "":
        discovered_devices = find_single_device(bacnet, TARGET_IP_ADDRESS)
    
    elif BACNET_NETWORKS == "":
        if BACNET_DEVICE_ID == "":
            if BACNET_RANGE != "":
                BACNET_RANGE_START = BACNET_RANGE.split(",")[0]
                BACNET_RANGE_FINISH = BACNET_RANGE.split(",")[1]
                print("start:", BACNET_RANGE_START)
                print("finish:", BACNET_RANGE_FINISH)
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_RANGE_START,BACNET_RANGE_FINISH))
                # discovered_devices = (set(bacnet.discoveredDevices.keys()))
                discovered_devices = bacnet.devices
            else:
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN) 
                # discovered_devices = (set(bacnet.discoveredDevices.keys()))
                discovered_devices = bacnet.devices
        else:
            discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID))
            # discovered_devices = (set(bacnet.discoveredDevices.keys()))
            discovered_devices = bacnet.devices
    else:
        bacnet_networks = string_to_integer_list(BACNET_NETWORKS)
        if BACNET_DEVICE_ID == "":
            if BACNET_RANGE != "":
                BACNET_RANGE_START = BACNET_RANGE.split(",")[0]
                BACNET_RANGE_FINISH = BACNET_RANGE.split(",")[1]
                print("start:", BACNET_RANGE_START)
                print("finish:", BACNET_RANGE_FINISH)
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_RANGE_START,BACNET_RANGE_FINISH), networks=bacnet_networks)
                # discovered_devices = (set(bacnet.discoveredDevices.keys()))
                discovered_devices = bacnet.devices
            else:
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, networks=bacnet_networks) 
                # discovered_devices = (set(bacnet.discoveredDevices.keys()))
                discovered_devices = bacnet.devices
        elif BACNET_DEVICE_ID != "":
            BACNET_DEVICE_ID = int(BACNET_DEVICE_ID)
            discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID), networks=bacnet_networks)
            # discovered_devices = (set(bacnet.discoveredDevices.keys()))
            discovered_devices = bacnet.devices

    # print(discovered_devices)

    output_path = "bacnet_devices"

    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    devices_df = pd.DataFrame()
    
    # if DEVICE_ONLY_SCAN:
    #     discovered_devices = (
    #               set(bacnet.discoveredDevices.keys())
    #           )
              
    # pprint(bacnet.devices)

    bacnet_devices_df = pd.DataFrame(discovered_devices, columns=['device_name', 'manufacturer', 'address', 'device_id'])
    bacnet_devices_df.index.name = "number"
    bacnet_devices_df.to_csv(os.path.join(output_path, "%s_devicelist_simple.csv" % SHEET_FILENAME_NAME))

    for device in discovered_devices:
        try:
            device_data = make_device_info_simple(output_path, args.verbose, device, network=bacnet)
            if not device_data.empty:
                devices_df = pd.concat([devices_df, device_data], ignore_index=True, axis=1)
        except Exception as e:
            print(f"Critical error processing device {device[3]}: {e}")
            continue
        
    # pprint(devices_df)

    #for index, row in devices_info.iterrows():
    devices_df = devices_df.transpose()
    devices_df.index.name = "number"
    print(tabulate(devices_df, headers='keys', tablefmt='psql'))
    devices_df.to_csv(os.path.join(output_path, "%s_devicelist.csv" % SHEET_FILENAME_NAME))

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
        
        #devices_df = devices_df.transpose()
        #devices_df.index.name = "number"
        #print(tabulate(devices_df, headers='keys', tablefmt='psql'))
        #devices_df.to_csv(os.path.join(output_path, "%s.csv" % SHEET_FILENAME_NAME))

        # devices, devices_info, points = create_data(output_path, args.verbose, bacnet.devices, network=bacnet, devicesonly=DEVICE_ONLY_SCAN)
        devices, points = create_data(output_path, args.verbose, bacnet.devices, network=bacnet, devicesonly=DEVICE_ONLY_SCAN)
        
        # #print(tabulate(devices_info, headers='keys', tablefmt='psql'))
        # devices_df = pd.DataFrame()
        # #for index, row in devices_info.iterrows():
        # for key, value in devices_info.items():
        #     devices_df = pd.concat( [devices_df, value], ignore_index=True, axis=1)
        # #devices_df = pd.DataFrame.from_dict(devices_info, orient='index')
        # devices_df = devices_df.transpose()
        # devices_df.index.name = "number"
        # print(tabulate(devices_df, headers='keys', tablefmt='psql'))

        make_sheet(devices_df, points, os.path.join(output_path, SHEET_FILENAME))

if __name__ == "__main__":
    main()

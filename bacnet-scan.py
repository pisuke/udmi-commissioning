#!/usr/bin/env python3

"""
  bacnet-scan.py

  Executes a BACnet scan and saves the results to a spreadsheet.

  [Usage] 
  python3 bacnet-scan.py [options]
"""

__author__ = "Francesco Anselmo"
__copyright__ = "Copyright 2025"
__credits__ = ["Francesco Anselmo", "Viktoras Cesnulevicius"]
__license__ = "MIT"
__version__ = "0.44"
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
import logging
import time

def show_title():
    """Show the program title and version info
    """

    title = r""" 
     ____    _    ____            _                           
    | __ )  / \  / ___|_ __   ___| |_      ___  ___ __ _ _ __  
    |  _ \ / _ \| |   | '_ \ / _ \ __|____/ __|/ __/ _` | '_ \ 
    | |_) / ___ \ |___| | | |  __/ ||_____\__ \ (_| (_| | | | |
    |____/_/   \_\____|_| |_|\___|\__|    |___/\___\__,_|_| |_|
    """

    print(title)
    print("-" * 60)
    print(f"Author:     {__author__}")
    print(f"Copyright:  {__copyright__}")
    print(f"License:    {__license__}")
    print(f"Version:    {__version__}")
    print(f"Status:     {__status__}")
    print("-" * 60)

# -- Direct Device Discovery Function --
def find_single_device(bacnet, device_ip):
    """
    Finds a single BACnet device at a known IP address.
    """
    print(f"Sending targeted Who-Is to {device_ip}...")
    bacnet.whois_router_to_network(network=None, destination=device_ip)
    bacnet.whois(device_ip, global_broadcast=True)
    time.sleep(2)

    for name, manufacturer, address, instance_id in bacnet.devices:
        if address.startswith(device_ip):
            print(f"Device responded. Address: {address}, Instance ID: {instance_id}")
            return [(name, manufacturer, address, instance_id)]
    
    return []

# -- Device Discovery Function --
def discover_devices(bacnet, subnet_broadcast, interval, checks):
    """
    Discovers BACnet devices on a network using a stable discovery method.
    """
    print(f"Sending Who-Is to {subnet_broadcast} and entering stable discovery loop...")
    bacnet.whois_router_to_network(network=None, destination=subnet_broadcast)
    bacnet.whois(subnet_broadcast, global_broadcast=True)

    last_device_count = 0
    stability_counter = 0
    while stability_counter < checks:
        time.sleep(interval)
        
        current_device_count = len(bacnet.devices)
        if current_device_count > last_device_count:
            print(f"Found new device(s). Total now: {current_device_count}.")
            stability_counter = 0
        else:
            stability_counter += 1
        last_device_count = current_device_count
    
    print("BACnet discovery completed.")
    return bacnet.devices

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
        try:
            name, vendor, address, device_id = each
        except ValueError:
            address, device_id = each[0], each[1]
            name = f"Unknown_Device_{device_id}"
            vendor = "Unknown"

        custom_obj_list = None
        sanitized_dev_name = sanitize_device_name(name)

        try:
            devices[sanitized_dev_name] = BAC0.device(
                address, device_id, network, poll=0, object_list=custom_obj_list
            )
        except Exception as e:
            print(f"Skipping device {sanitized_dev_name} at {address} due to creation error: {e}")
            continue

        if not devicesonly:
            try:
                combined_id_name = f"{device_id}_{sanitized_dev_name}"
                points[combined_id_name] = make_points(output_path, verbose, devices[sanitized_dev_name], combined_id_name, sanitized_dev_name)
            except Exception as e:
                print(f"Skipping points for device {sanitized_dev_name} due to enumeration error: {e}")
                
    return (devices,points)

def make_device_info_simple(output_path, verbose, dev, network):
    lst = {}
    
    try:
        address, device_id = dev
    except:
        name, manufacturer, address, device_id = dev

    object_name = f"Unknown_{device_id}"
    vendor_name = ""
    firmware_version = ""
    model_name = ""
    serial_number = ""
    description = ""
    location = ""
    application_software_version = ""

    try:
        results = network.readMultiple(
            f"{address} device {device_id} objectName vendorName"
            " firmwareRevision modelName serialNumber description location applicationSoftwareVersion"
        )
        
        if results and len(results) == 8:
            object_name, vendor_name, firmware_version, model_name, serial_number, description, location, application_software_version = results
            
    except (BAC0.core.io.IOExceptions.SegmentationNotSupported, Exception) as err:
        print(f"Warning: error reading standard properties from {address}/{device_id}. Using fallbacks. ({err})")
    
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

    try:
        device = BAC0.device(address, device_id, network, poll=0)
    except Exception as e:
        print(f"Error initializing BAC0 device {name}: {e}")
        return pd.DataFrame() 
    
    try:
        description = device.bacnet_properties.get("description", "")
    except:
        description = ""
        
    try:
        location = device.bacnet_properties.get("location", "")
    except:
        location = ""

    try:
        application_software_version = device.bacnet_properties.get("applicationSoftwareVersion", "")
    except:
        application_software_version = ""
    
    try:
        firmware_revision = device.bacnet_properties.get("firmwareRevision", "")
    except:
        firmware_revision = ""
    
    try:
        vendor_name = device.bacnet_properties.get("vendorName", "")
    except:
        vendor_name = ""

    try:
        model_name = device.bacnet_properties.get("modelName", "")
    except:
        model_name = ""

    try:
        serial_number = device.bacnet_properties.get("serialNumber", "")
    except:
        serial_number = ""

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
        }
    df = pd.DataFrame.from_dict(lst, orient="index")
    df.index.name = "property"
    df.rename(columns={0: "value"}, inplace=True)
    if verbose:
        print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def make_points(output_path, verbose, dev, file_name_identifier, dev_name):
    lst = {}
    sanitized_dev_name = sanitize_device_name(dev_name)
    
    if not hasattr(dev, 'points'):
        print(f"No points found or accessible for {dev_name}")
        return pd.DataFrame()
        
    for each in dev.points:
        try:
            point_name = getattr(each.properties, 'name', 'unknown_point')
            units_state = getattr(each.properties, 'units_state', '')
            desc = getattr(each.properties, 'description', '')
            obj_type = getattr(each.properties, 'type', 'unknown')
            obj_address = getattr(each.properties, 'address', 'unknown')
            last_val = getattr(each, 'lastValue', '')
            
            lst[point_name] = {
                "device_name": dev_name,
                "sanitized_device_name": sanitized_dev_name,
                "value": last_val,
                "units_or_states": units_state,
                "description": desc,
                "object": f"{obj_type}:{obj_address}",
                "cloud_device_id": "",
                "cloud_point_name": "",
                "cloud_value": "",
                "validation_status": ""
            }
        except Exception as e:
            print(f"Warning: skipped reading a point on {dev_name} due to error: {e}")
            continue 
            
    df = pd.DataFrame.from_dict(lst, orient="index")
    df.index.name = "point_name"
    
    df.to_csv(os.path.join(output_path, "%s.csv" % file_name_identifier))
    if verbose:
        print(tabulate(df, headers='keys', tablefmt='psql'))
    return df

def sanitize_excel_sheet_name(input_string):
    """
    Sanitizes a string for use as an Excel worksheet tab name.
    Excel strictly enforces: max 31 chars, no [ ] : * ? / \ and cannot be 'History'
    """
    s = str(input_string)
    # Remove all characters that break Excel sheet names
    s = re.sub(r'[\\/*?:\[\]]', '_', s)
    
    if s.lower() == 'history':
        s = 'History_dev'
        
    if not s.strip():
        s = "Unknown_Device"
        
    # Enforce absolute 31 character limit
    s = s[:31]
    return s

def make_sheet(devices_df, dfs, sheet_filename):
    print("Compiling final Excel spreadsheet...")
    try:
        with pd.ExcelWriter(sheet_filename, engine='xlsxwriter') as writer:
            # 1. Sanitize the main "devices" tab too
            devices_df.to_excel(writer, sheet_name="devices_list")
            used_sheet_names = {"devices_list"}
            
            for k, v in dfs.items():
                # 2. Force conversion to string and sanitize
                raw_name = str(k)
                safe_sheet_name = sanitize_excel_sheet_name(raw_name)
                
                # 3. Handle duplicates
                original_safe = safe_sheet_name
                counter = 1
                while safe_sheet_name in used_sheet_names:
                    suffix = f"_{counter}"
                    # Ensure we leave room for the counter
                    safe_sheet_name = original_safe[:31-len(suffix)] + suffix
                    counter += 1
                    
                used_sheet_names.add(safe_sheet_name)
                
                try:
                    v.to_excel(writer, sheet_name=safe_sheet_name)
                except Exception as e:
                    print(f"Could not write sheet '{safe_sheet_name}' (from '{raw_name}'): {e}")
                    pass
        print(f"Devices point lists written successfully to file {sheet_filename}")
    except Exception as e:
        print(f"Failed to finalize the Excel file. Details: {e}")

def sanitize_unix_command(input_string):
    offending_unix_chars = r"[;&|<>`'$(){}\[\]#\s:/]"
    sanitized_string = ""
    try:
        sanitized_string = re.sub(offending_unix_chars, "_", str(input_string))
    except:
        sanitized_string = "Unknown"
    return sanitized_string  

def sanitize_spreadsheet_tabs(input_string):
    sanitized_string = str(input_string).replace('\t', '_')
    return sanitized_string  

def sanitize_device_name(input_string):
    sanitized_unix = sanitize_unix_command(input_string)
    sanitized_both = sanitize_spreadsheet_tabs(sanitized_unix)
    return sanitized_both

def string_to_integer_list(comma_separated_string):
  if not comma_separated_string.strip():
    return []

  string_numbers = comma_separated_string.split(',')
  integer_list = []
  for s in string_numbers:
    try:
      number = int(s.strip()) 
      integer_list.append(number)
    except ValueError:
      print(f"Warning: Invalid integer format: '{s.strip()}', skipping.")
  return integer_list
    
def should_exclude_device(device, exclude_list):
    """Checks if a discovered device should be excluded from the scan.
    """
    if not exclude_list:
        return False
    
    try:
        if len(device) == 2:
            address, device_id = device
        else:
            name, manufacturer, address, device_id = device
    except Exception:
        return False
        
    device_id_str = str(device_id).strip()
    
    for excl in exclude_list:
        excl = excl.strip()
        if not excl:
            continue
        if device_id_str == excl:
            return True
            
    return False
    
def main():
    show_title()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False, help="increase the verbosity level (optional)")
    parser.add_argument("-x", "--export",  default="bacnet-scan.xlsx", help="spreadsheet file name for scan results")
    parser.add_argument("-a", "--address", default="", help="IP address of BACnet interface in Mango (optional)")
    parser.add_argument("-n", "--networks", default="", help="comma separated target list of BACnet networks (optional)")
    parser.add_argument("-b", "--bacnetid", default="", help="restrict the scan to only one device with this BACnet ID")
    parser.add_argument("-r", "--range", default="",  help="restrict the scan to a device range")
    parser.add_argument("-d", "--deviceonly", action="store_true", default=False, help="only execute a BACnet WHOIS scan")
    parser.add_argument("-g", "--globalscan", action="store_true", default=False, help="execute a global broadcast BACnet scan")
    parser.add_argument("-s", "--subnet_broadcast", default="", help="restrict the scan to a specific subnet broadcast address")
    parser.add_argument("-i", "--ip", default="", help="restrict the scan to a specific device with this IP address")
    parser.add_argument("-e", "--exclude", default="", help="comma separated list of BACnet device IDs to exclude from scan (optional)")

    args = parser.parse_args()

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
    else:
        BAC0.log_level("silence")
        pass


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
    POLLING_INTERVAL = 5
    STABILITY_CHECKS = 3

    print(("Bacnet Global Scan:", BACNET_GLOBAL_SCAN))
    print("Initializing BAC0 client...")
    
    try:
        if BACNET_IP_ADDRESS != "":        
            bacnet = BAC0.lite(ip=BACNET_IP_ADDRESS, deviceId=4194301, modelName="bacnet-scan")
        else:
            bacnet = BAC0.lite(deviceId=4194301, modelName="bacnet-scan")
    except Exception as e:
        print(f"Failed to initialize BAC0 client: {e}")
        sys.exit(1)

    # Step 1: Discover Devices
    try:
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
                    discovered_devices = bacnet.devices
                else:
                    discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN) 
                    discovered_devices = bacnet.devices
            else:
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID))
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
                    discovered_devices = bacnet.devices
                else:
                    discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, networks=bacnet_networks) 
                    discovered_devices = bacnet.devices
            elif BACNET_DEVICE_ID != "":
                BACNET_DEVICE_ID = int(BACNET_DEVICE_ID)
                discover = bacnet.discover(global_broadcast=BACNET_GLOBAL_SCAN, limits=(BACNET_DEVICE_ID,BACNET_DEVICE_ID), networks=bacnet_networks)
                discovered_devices = bacnet.devices
    except Exception as e:
        print(f"Discovery phase encountered a critical error: {e}")
        discovered_devices = getattr(bacnet, 'devices', [])

    exclude_list = [x.strip() for x in args.exclude.split(',')] if args.exclude else []
    if exclude_list:
        filtered_devices = []
        for dev in discovered_devices:
            if should_exclude_device(dev, exclude_list):
                print(f"Excluding device from scan: {dev}")
                continue
            filtered_devices.append(dev)
        discovered_devices = filtered_devices

    output_path = "bacnet_devices"

    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    devices_df = pd.DataFrame()
    
    try:
        bacnet_devices_df = pd.DataFrame(discovered_devices, columns=['device_name', 'manufacturer', 'address', 'device_id'])
        bacnet_devices_df.index.name = "number"
        bacnet_devices_df.to_csv(os.path.join(output_path, "%s_devicelist_simple.csv" % SHEET_FILENAME_NAME))
    except Exception as e:
        print(f"Could not save simple device list: {e}")

    for device in discovered_devices:
        try:
            address = device[0]
            device_id = device[1]
            dev_info = make_device_info_simple(output_path, args.verbose, device, network=bacnet)
            if not dev_info.empty:
                devices_df = pd.concat([devices_df, dev_info], ignore_index=True, axis=1)
        except Exception as e:
            print(f"Error processing preliminary info for device {device}: {e}")
            continue
        
    if not devices_df.empty:
        devices_df = devices_df.transpose()
        devices_df.index.name = "number"
        print(tabulate(devices_df, headers='keys', tablefmt='psql'))
        devices_df.to_csv(os.path.join(output_path, "%s_devicelist.csv" % SHEET_FILENAME_NAME))
    
    if not DEVICE_ONLY_SCAN:
        devices, points = create_data(output_path, args.verbose, discovered_devices, network=bacnet, devicesonly=DEVICE_ONLY_SCAN)
        make_sheet(devices_df, points, os.path.join(output_path, SHEET_FILENAME))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScan aborted by user.")
    except Exception as e:
        print(f"\nScan encountered a fatal error: {e}")
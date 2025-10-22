#!/usr/bin/env python3

"""
  sheet2mangojson.py

  Converts a spreadsheet including BACnet information from bacnet-scan
  to a Mango JSON input file.

  [Usage] 
  python3 sheet2mangojson.py [options]
"""

__author__ = "Francesco Anselmo"
__copyright__ = "Copyright 2025"
__credits__ = ["Francesco Anselmo"]
__license__ = "MIT"
__version__ = "0.2" # Updated version to reflect changes
__maintainer__ = "Francesco Anselmo"
__email__ = "francesco.anselmo@gmail.com"
__status__ = "Dev"

from pprint import pprint
import argparse
import pandas as pd
# import BAC0 # Unused in the provided script, can be removed if not needed elsewhere
from tabulate import tabulate # Unused in the provided script, can be removed if not needed elsewhere
import os
import socket
import ipaddress
from string import Template
# from math import isnan # Replaced with pandas.isna or native checks
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend


TEMPLATE_BACNET_LOCALDEVICE = Template('''
"BACnetLocalDevices": [
   {
     "baudRate": 9600,
     "broadcastAddress": "${broadcast_address}",
     "commPortId": null,
     "configProgramLocation": "mstp-config.o",
     "deviceId": ${bacnet_localdevice_id},
     "deviceName": "BACnet Local Device",
     "driverFileLocation": "mstp.ko",
     "foreignBBMDAddress": "",
     "foreignBBMDPort": 47808,
     "foreignBBMDTimeToLive": 300,
     "id": "BNLD_config",
     "localBindAddress": "0.0.0.0",
     "localNetworkNumber": 0,
     "maxInfoFrames": 1,
     "maxMaster": 127,
     "port": 47808,
     "responseTimeoutMs": 1000,
     "retries": 2,
     "retryCount": 1,
     "reuseAddress": false,
     "segTimeout": 5000,
     "segWindow": 5,
     "subnet": 24,
     "thisStation": 0,
     "timeout": 6000,
     "type": "ip",
     "usageTimeout": 20,
     "useRealtime": false
   }
]''')

TEMPLATE_BACNET_DATASOURCE = Template("""
    {
      "xid": "${data_source_xid}",
      "name": "${data_source_name}",
      "enabled": false,
      "type": "BACnetIP",
      "alarmLevels": {
        "INITIALIZATION_EXCEPTION": "URGENT",
        "POLL_ABORTED": "URGENT",
        "DEVICE_EXCEPTION": "URGENT",
        "MESSAGE_EXCEPTION": "URGENT"
      },
      "purgeType": "YEARS",
      "updatePeriods": 1,
      "updatePeriodType": "MINUTES",
      "covSubscriptionTimeoutMinutes": 60,
      "localDeviceConfig": "BNLD_config",
      "quantize": false,
      "useCron": false,
      "data": null,
      "editPermission": [
        [
          "superadmin"
        ]
      ],
      "purgeOverride": false,
      "purgePeriod": 1,
      "readPermission": []
    }""")

TEMPLATE_BACNET_DATAPOINT = Template("""
{
    "name": "${mango_point_name}",
    "enabled": true,
    "loggingType": "INTERVAL",
    "intervalLoggingPeriodType": "MINUTES",
    "intervalLoggingType": "AVERAGE",
    "purgeType": "YEARS",
    "pointLocator": {
    "dataType": "${point_data_type}",
    "objectType": "${point_object_type}",
    "propertyIdentifier": "present-value",
    "additive": 0,
    "multiplier": 1,
    "objectInstanceNumber": ${point_remote_object_instance_number},
    "remoteDeviceInstanceNumber": ${point_remote_device_instance_number},
    "settable": false,
    "useCovSubscription": false,
    "writePriority": 16
    },
    "eventDetectors": [],
    "plotType": "SPLINE",
    "rollup": "NONE",
    "unit": "",
    "simplifyType": "NONE",
    "chartColour": "",
    "data": null,
    "dataSourceXid": "${data_source_xid}",
    "defaultCacheSize": 1,
    "deviceName": "${mango_device_name}",
    "discardExtremeValues": false,
    "discardHighLimit": 1.7976931348623157e+308,
    "discardLowLimit": -1.7976931348623157e+308,
    "editPermission": [],
    "intervalLoggingPeriod": 1,
    "intervalLoggingSampleWindowSize": 0,
    "overrideIntervalLoggingSamples": false,
    "preventSetExtremeValues": false,
    "purgeOverride": false,
    "purgePeriod": 1,
    "readPermission": [],
    "setExtremeHighLimit": 1.7976931348623157e+308,
    "setExtremeLowLimit": -1.7976931348623157e+308,
    "setPermission": [],
    "tags": {
      "BACnetDeviceName": "${bacnet_device_name}",
      "BACnetObjectName": "${bacnet_point_name}",
      "BACnetPropertyName": "${bacnet_point_property_name}",
      "BACnetObjectDescription": "${bacnet_point_description}"
    },
    "textRenderer": {
    "type": "ANALOG",
    "useUnitAsSuffix": false,
    "suffix": "",
    "format": "0.00"
    },
    "tolerance": 0,
    "xid": "${mango_point_xid}"
}""")

TEMPLATE_MANGO_SYSTEM_SETTINGS = Template("""
  "systemSettings": {
    "udmi.config": "{\\"iotProvider\\": \\"GBOS\\", \\"projectId\\": \\"${gcp_project_id}\\", \\"cloudRegion\\": \\"${gcp_cloud_region}\\", \\"registryId\\": \\"${registry_id}\\", \\"site\\": \\"${site_id}\\"}"
  },
""")

TEMPLATE_MANGO_UDMI_PUBLISHER = Template("""
{
    "xid": "${mango_publisher_xid}",
    "name": "${mango_publisher_name}",
    "enabled": false,
    "type": "UDMI",
    "snapshotSendPeriodType": "MINUTES",
    "publishType": "ALL",
    "alarmLevels": {
    "POINT_DISABLED_EVENT": "URGENT",
    "QUEUE_SIZE_WARNING_EVENT": "URGENT"
    },
    "gateway": true,
    "proxyDevices": ${mango_proxydevices_array},
    "privateKey": "${mango_rsa_privatekey}",
    "publicKey": "${mango_rsa_publickey}",
    "cacheDiscardSize": 50000,
    "cacheWarningSize": 10000,
    "publishAttributeChanges": false,
    "publishPointEvents": false,
    "sendSnapshot": false,
    "snapshotSendPeriods": 5
}""")

TEMPLATE_MANGO_PUBLISHED_POINT = Template('''
    {
      "name": "${point_name}",
      "enabled": true,
      "dataPointXid": "${point_xid}",
      "deviceName": "${device_name}",
      "publisherXid": "${mango_publisher_xid}"
    }''')

def show_title():
    """Show the program title"""

    title = r"""
     _               _   ____  
 ___| |__   ___  ___| |_|___ \ 
/ __| '_ \ / _ \/ _ \ __| __) |
\__ \ | | |  __/  __/ |_ / __/ 
|___/_| |_|\___|\___|\__|_____|
                                   _                 
 _ __ ___   __ _ _ __   __ _  ___ (_)___  ___  _ __  
| '_ ` _ \ / _` | '_ \ / _` |/ _ \| / __|/ _ \| '_ \ 
| | | | | | (_| | | | | (_| | (_) | \__ \ (_) | | | |
|_| |_| |_|\__,_|_| |_|\__, |\___// |___/\___/|_| |_|
                       |___/    |__/                 

    """

    print(title)


def get_broadcast_address():
  """
  Gets the broadcast address of the current machine.

  Returns:
      str: The broadcast address as a string.
  """
  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))  # Connect to Google DNS to get the local IP
    local_ip = s.getsockname()[0]
    # Use ip_interface to correctly determine the broadcast address (assuming /24 subnet based on config)
    # The original script does not specify subnet mask, but uses the local IP. 
    # Let's assume a default mask for the interface if the user didn't specify one.
    # The original script was implicitly doing this; using ip_network(local_ip + '/24') 
    # would be more explicit if the user's local IP had no netmask attached, 
    # but the current use of ip_interface(local_ip) is safer for general use.
    ip_address = ipaddress.ip_interface(local_ip)
    return str(ip_address.network.broadcast_address)
  except Exception as e:
    print(f"Error getting broadcast address: {e}")
    return None
  
def add_to_list_if_not_exists(my_list, new_entry):
  """
  Adds a new entry to the list if it doesn't already exist.
  (Kept for compatibility, though a set is more efficient for uniqueness)

  Args:
    my_list: The list to add the entry to.
    new_entry: The entry to be added.

  Returns:
    The updated list.
  """
  if new_entry not in my_list:
    my_list.append(new_entry)
  return my_list

def main():
    show_title()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="increase the verbosity level",
    )
    parser.add_argument(
        "-u",
        "--unique",
        action="store_true",
        default=False,
        help="create unique BACNET data sources per sanitized_device_name",
    )
    parser.add_argument(
        "-i",
        "--input",
        default="",
        help="file name of the spreadsheet including the scan results",
    )
    parser.add_argument(
        "-o", "--output", default="", help="file name prefix of the output Mango JSON files"
    )
    parser.add_argument(
        "-l", "--localdevice", default="98765", help="ID of the BACnet Local Device to use in Mango"
    )
    parser.add_argument(
        "-b", "--broadcast", default="255.255.255.255", help="Broadcast address for the BACnet Local Device to use in Mango"
    )
    parser.add_argument(
        "-p", "--publisher", default="CGW-1", help="Name to use for the Mango UDMI publisher device (default is CGW-1)"
    )
    parser.add_argument(
        "-j", "--project", default="gcp-project-name", help="GCP project name for the UDMI publisher"
    )
    parser.add_argument(
        "-g", "--region", default="us-central1", help="GCP region for the UDMI publisher"
    )
    parser.add_argument(
        "-r", "--registry", default="ZZ-ABC-DEF", help="IoT Core registry ID for the UDMI publisher"
    )
    parser.add_argument(
        "-s", "--site", default="ZZ-ABC-DEF", help="IoT Core site name for the UDMI publisher"
    )
    
    args = parser.parse_args()
    
    bacnet_localdevice_id = args.localdevice
    broadcast_address = args.broadcast 
    publisher_name = args.publisher
    
    gcp_project_id = args.project
    gcp_cloud_region = args.region
    registry_id = args.registry
    site_id = args.site

    if args.verbose:
        print("program arguments:")
        print(args)

    if args.input and args.output and os.path.exists(args.input):
        
        # --- Start of Efficiency Improvement 1: Read all sheets once ---
        try:
            print(f"Reading all sheets from {args.input}...")
            xls = pd.ExcelFile(args.input)
            sheets = xls.sheet_names
            # Read all sheets into a dictionary of DataFrames
            all_data = {sheet: xls.parse(sheet) for sheet in sheets}
            print("Finished reading sheets.")
        except Exception as e:
            print(f"Error reading file {args.input}: {e}")
            return # Exit if file cannot be read
            
        print(f"Sheets found in {args.input}:")
        for sheet in sheets:
            print(f"- {sheet}")
            
        print()
        
        if "devices" not in all_data:
            print("Error: Sheet 'devices' not found in the file.")
            return # Exit if the critical 'devices' sheet is missing

        devices_data = all_data["devices"]
        print(f"Data from sheet 'devices':")
        print(devices_data)
        
        print()
        
        # Filter devices data to exclude "BAC0" and ensure 'sanitized_device_name' is not empty/NaN
        valid_devices_data = devices_data[devices_data['sanitized_device_name'].notna() & (devices_data['sanitized_device_name'] != "BAC0")]
        
        output_mango_bacnet_config_filename = f"{args.output}_bacnet_config.json"
        output_mango_udmi_publisher_filename = f"{args.output}_udmi_publisher.json"
        
        # --- UDMI Key Generation and initial file setup ---
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        
        private_key = key.private_bytes(
            encoding=crypto_serialization.Encoding.PEM,
            format=crypto_serialization.PrivateFormat.PKCS8,
            encryption_algorithm=crypto_serialization.NoEncryption()
        )

        public_key = key.public_key().public_bytes(
            encoding=crypto_serialization.Encoding.PEM,
            format=crypto_serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        # --- Start of Efficiency Improvement 2 & 3: Consolidated proxy device collection and point count ---
        proxy_devices = set()
        points_to_be_exported = 0
        
        # Iterate over valid devices and collect points data efficiently
        for index, device in valid_devices_data.iterrows():
            sheet_name = device['sanitized_device_name']
            if sheet_name in all_data:
                points_data = all_data[sheet_name]
                
                # Filter points where cloud_device_id and cloud_point_name are valid
                valid_points = points_data[
                    points_data['cloud_device_id'].notna() & 
                    points_data['cloud_point_name'].notna()
                ]
                
                # Update counters and unique device IDs
                points_to_be_exported += len(valid_points)
                proxy_devices.update(valid_points['cloud_device_id'].tolist())
            
        print("Proxy devices: ", sorted(list(proxy_devices)))
        
        mango_proxydevices_array_string = "[" + ",".join([
            '{"name": "%s"}' % proxy_device for proxy_device in sorted(list(proxy_devices))
        ]) + "]"

        mango_publisher_xid = f"PUB_UDMI_BACNET_{publisher_name}"
        mango_publisher_name = publisher_name
        # --- End of Efficiency Improvement 2 & 3 ---

        
        # --- Write UDMI Publisher File ---
        with open(output_mango_udmi_publisher_filename, 'w') as output_mango_udmi_publisher_file:
            output_mango_udmi_publisher_file.write('{\n')
            
            output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_SYSTEM_SETTINGS.substitute(
                gcp_project_id=gcp_project_id,
                gcp_cloud_region=gcp_cloud_region,
                registry_id=registry_id,
                site_id=site_id
            ))
            
            output_mango_udmi_publisher_file.write('\n  "publishers": [\n')
            
            output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_UDMI_PUBLISHER.substitute(
                mango_publisher_xid=mango_publisher_xid,
                mango_publisher_name=mango_publisher_name,
                mango_proxydevices_array=mango_proxydevices_array_string,
                # Decode and escape keys for JSON insertion
                mango_rsa_privatekey=private_key.decode('utf-8').replace("\n", "\\n"),
                mango_rsa_publickey=public_key.decode('utf-8').replace("\n", "\\n")
            ))
            
            output_mango_udmi_publisher_file.write('],"publishedPoints": [')

            # --- Write BACnet Config File header (Local Device and Data Sources) ---
            with open(output_mango_bacnet_config_filename, 'w') as output_mango_bacnet_config_file:
                output_mango_bacnet_config_file.write("{\n")
                output_mango_bacnet_config_file.write(TEMPLATE_BACNET_LOCALDEVICE.substitute(broadcast_address=broadcast_address, bacnet_localdevice_id=bacnet_localdevice_id))
                output_mango_bacnet_config_file.write(",\n")
                
                # --- Data Source Logic ---
                output_mango_bacnet_config_file.write('"dataSources": [\n')
                if args.unique:
                    unique_device_names = sorted(list(valid_devices_data['sanitized_device_name'].unique()))
                    for i, device_name in enumerate(unique_device_names):
                        data_source_xid = f"DS_BACNET_{device_name}"
                        data_source_name = f"BACNET_{device_name}"
                        output_mango_bacnet_config_file.write(TEMPLATE_BACNET_DATASOURCE.substitute(data_source_xid=data_source_xid, data_source_name=data_source_name))
                        if i < len(unique_device_names) - 1:
                            output_mango_bacnet_config_file.write(",\n")
                else:
                    # Default behavior: single data source
                    data_source_xid = "DS_BACNET"
                    data_source_name = "BACNET"
                    output_mango_bacnet_config_file.write(TEMPLATE_BACNET_DATASOURCE.substitute(data_source_xid=data_source_xid, data_source_name=data_source_name))
                output_mango_bacnet_config_file.write("\n],")
                
                output_mango_bacnet_config_file.write('\n"dataPoints": [\n')
                
                # --- Data Point and Published Point Generation ---
                
                OBJECT_TYPE = {
                  'analogInput': 'ANALOG_INPUT',
                  'analogOutput': 'ANALOG_OUTPUT',
                  'analogValue': 'ANALOG_VALUE',
                  'binaryInput': 'BINARY_INPUT',
                  'binaryOutput': 'BINARY_OUTPUT',
                  'binaryValue': 'BINARY_VALUE',
                  'multiStateInput': 'MULTISTATE_INPUT',
                  'multiStateOutput': 'MULTISTATE_OUTPUT',
                  'multiStateValue': 'MULTISTATE_VALUE'
                }
                
                i = 0
                
                for index, device in valid_devices_data.iterrows():
                    sheet_name = device['sanitized_device_name']
                    if sheet_name in all_data:
                        print(f"\nProcessing device: {sheet_name} ({device['device_id']})")
                        points_data = all_data[sheet_name]
                        
                        # Use the pre-filtered valid points set
                        valid_points = points_data[
                            points_data['cloud_device_id'].notna() & 
                            points_data['cloud_point_name'].notna()
                        ]

                        # Determine the data source XID based on the --unique flag
                        current_data_source_xid = f"DS_BACNET_{sheet_name}" if args.unique else "DS_BACNET"
                    
                        for _, point in valid_points.iterrows():
                            i += 1
                            
                            cloud_device_id = point['cloud_device_id']
                            cloud_point_name = point['cloud_point_name']

                            point_data_type = "NUMERIC" # Based on original assumption for all point types
                            
                            mango_point_name = cloud_point_name
                            mango_device_id = cloud_device_id
                            
                            try:
                                # Safe split, assumes format "type:instance"
                                object_parts = point['object'].split(":")
                                point_remote_object_instance_number = object_parts[1]
                                point_object_type_str = object_parts[0]
                                point_object_type = OBJECT_TYPE.get(point_object_type_str, 'UNKNOWN')
                            except (IndexError, AttributeError):
                                print(f"Warning: Skipping point with invalid 'object' format: {point['object']}")
                                continue # Skip invalid points

                            point_remote_device_instance_number = device['device_id']

                            bacnet_point_property_name = point_object_type
                            # Use pandas.isna() for robust NaN check
                            bacnet_point_description = "" if pd.isna(point['description']) else point['description']
                            
                            # Generate unique XID
                            mango_point_xid = f"DP_{device['device_id']}_{point_object_type}_{point_remote_object_instance_number}"
                            bacnet_point_name = point['point_name']
                            bacnet_device_name = point['device_name']
                            
                            if args.verbose:
                                print(f"  {mango_device_id}, {bacnet_device_name}, {cloud_point_name}, {bacnet_point_name}, {mango_point_xid}")
                                
                            # Write BACnet Data Point
                            output_mango_bacnet_config_file.write(TEMPLATE_BACNET_DATAPOINT.substitute(
                                                mango_point_name=mango_point_name, 
                                                point_data_type=point_data_type,
                                                mango_device_name = mango_device_id,
                                                bacnet_point_name=bacnet_point_name,
                                                point_object_type=point_object_type,
                                                point_remote_object_instance_number=point_remote_object_instance_number,
                                                point_remote_device_instance_number=point_remote_device_instance_number,
                                                bacnet_device_name=bacnet_device_name,
                                                bacnet_point_property_name=bacnet_point_property_name,
                                                bacnet_point_description=bacnet_point_description,
                                                mango_point_xid=mango_point_xid,
                                                data_source_xid=current_data_source_xid
                                                )
                                    )
                            
                            # Write Published Point
                            output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_PUBLISHED_POINT.substitute(
                                    point_name=mango_point_name,
                                    point_xid=mango_point_xid,
                                    device_name=mango_device_id,
                                    mango_publisher_xid=mango_publisher_xid
                            ))
                            
                            # Add comma separators
                            if i < points_to_be_exported:
                                output_mango_bacnet_config_file.write(',\n')
                                output_mango_udmi_publisher_file.write(',\n')
                        
                # Close files
                output_mango_bacnet_config_file.write("]\n}")
            output_mango_udmi_publisher_file.write("]\n}")
            
    else:
        print("Please provide both input and output filename prefix, and ensure the input file exists.")


if __name__ == "__main__":
    main()
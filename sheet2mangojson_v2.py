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
__version__ = "0.1"
__maintainer__ = "Francesco Anselmo"
__email__ = "francesco.anselmo@gmail.com"
__status__ = "Dev"

from pprint import pprint
import argparse
import pandas as pd
import os
import socket
import ipaddress
from string import Template
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend
import json

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


def get_all_sheets_from_file(file_path):
    """
    Loads all sheets from an XLSX or ODS file into a dictionary of DataFrames.

    Args:
      file_path: Path to the XLSX or ODS file.

    Returns:
      A dictionary of pandas DataFrames, or an empty dictionary if an error occurs.
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        all_sheets_data = pd.read_excel(excel_file, sheet_name=None)
        return all_sheets_data
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}


def get_broadcast_address():
    """
    Gets the broadcast address of the current machine.

    Returns:
        str: The broadcast address as a string.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        ip_address = ipaddress.ip_interface(local_ip)
        return str(ip_address.network.broadcast_address)
    except Exception as e:
        print(f"Error getting broadcast address: {e}")
        return None


def isNaN(num):
    return num != num


def main():
    show_title()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-v", "--verbose", action="store_true", default=False, help="increase the verbosity level"
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

    if not (args.input and args.output and os.path.exists(args.input)):
        print("Please provide both input and output filename prefix, and ensure the input file exists.")
        return

    # Read all sheets from the Excel file in a single operation
    all_sheets_data = get_all_sheets_from_file(args.input)
    if not all_sheets_data or "devices" not in all_sheets_data:
        print("Error: 'devices' sheet not found or file could not be read.")
        return

    devices_data = all_sheets_data["devices"]
    if args.verbose:
        print("program arguments:")
        print(args)
        print("\nData from sheet 'devices':")
        print(devices_data)

    broadcast_address = args.broadcast
    if broadcast_address == "255.255.255.255":
        broadcast_address = get_broadcast_address() or "255.255.255.255"

    output_mango_bacnet_config_filename = f"{args.output}_bacnet_config.json"
    output_mango_udmi_publisher_filename = f"{args.output}_udmi_publisher.json"

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

    # Prepare data for JSON generation
    bacnet_data_points = []
    udmi_published_points = []
    proxy_devices = set()

    for _, device in devices_data.iterrows():
        sanitized_device_name = device.get('sanitized_device_name')
        if not sanitized_device_name or sanitized_device_name == "BAC0" or sanitized_device_name not in all_sheets_data:
            continue

        points_data = all_sheets_data[sanitized_device_name]
        print(f"\nProcessing {sanitized_device_name}:")

        # Determine the data source XID based on the --unique flag
        current_data_source_xid = f"DS_BACNET_{sanitized_device_name}" if args.unique else "DS_BACNET"
        
        for _, point in points_data.iterrows():
            cloud_device_id = point.get('cloud_device_id')
            cloud_point_name = point.get('cloud_point_name')
            
            if pd.notna(cloud_device_id) and pd.notna(cloud_point_name):
                proxy_devices.add(cloud_device_id)

                point_data_type = "NUMERIC"
                mango_point_name = cloud_point_name
                mango_device_id = cloud_device_id
                
                try:
                    point_remote_object_instance_number = int(point['object'].split(":")[1])
                    point_remote_device_instance_number = int(device['device_id'])
                    point_object_type = OBJECT_TYPE.get(point['object'].split(":")[0])
                except (IndexError, ValueError):
                    print(f"Skipping malformed object for point {cloud_point_name}")
                    continue

                if not point_object_type:
                    print(f"Skipping unknown object type for point {cloud_point_name}")
                    continue
                
                bacnet_point_property_name = point_object_type
                bacnet_point_description = point['description'] if pd.notna(point.get('description')) else ""
                
                mango_point_xid = f"DP_{point_remote_device_instance_number}_{point_object_type}_{point_remote_object_instance_number}"
                bacnet_point_name = point.get('point_name', '')
                bacnet_device_name = point.get('device_name', '')

                # Prepare the data point dictionary
                bacnet_point_dict = {
                    "mango_point_name": mango_point_name,
                    "point_data_type": point_data_type,
                    "mango_device_name": mango_device_id,
                    "bacnet_point_name": bacnet_point_name,
                    "point_object_type": point_object_type,
                    "point_remote_object_instance_number": point_remote_object_instance_number,
                    "point_remote_device_instance_number": point_remote_device_instance_number,
                    "bacnet_device_name": bacnet_device_name,
                    "bacnet_point_property_name": bacnet_point_property_name,
                    "bacnet_point_description": bacnet_point_description,
                    "mango_point_xid": mango_point_xid,
                    "data_source_xid": current_data_source_xid
                }
                bacnet_data_points.append(bacnet_point_dict)

                # Prepare the published point dictionary
                udmi_point_dict = {
                    "point_name": mango_point_name,
                    "point_xid": mango_point_xid,
                    "device_name": mango_device_id,
                    "mango_publisher_xid": f"PUB_UDMI_{args.publisher}"
                }
                udmi_published_points.append(udmi_point_dict)

    # Generate JSON strings
    bacnet_localdevice_section = TEMPLATE_BACNET_LOCALDEVICE.substitute(
        broadcast_address=broadcast_address, bacnet_localdevice_id=args.localdevice
    )

    if args.unique:
        unique_device_names = sorted(list(set(d['sanitized_device_name'] for _, d in devices_data.iterrows() if d.get('sanitized_device_name') not in ["BAC0"])))
        data_source_sections = [
            TEMPLATE_BACNET_DATASOURCE.substitute(data_source_xid=f"DS_BACNET_{name}", data_source_name=f"BACNET_{name}")
            for name in unique_device_names
        ]
    else:
        data_source_sections = [TEMPLATE_BACNET_DATASOURCE.substitute(data_source_xid="DS_BACNET", data_source_name="BACNET")]

    bacnet_data_points_sections = [
        TEMPLATE_BACNET_DATAPOINT.substitute(**point) for point in bacnet_data_points
    ]
    
    # UDMI publisher
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = key.private_bytes(encoding=crypto_serialization.Encoding.PEM, format=crypto_serialization.PrivateFormat.PKCS8, encryption_algorithm=crypto_serialization.NoEncryption())
    public_key_pem = key.public_key().public_bytes(encoding=crypto_serialization.Encoding.PEM, format=crypto_serialization.PublicFormat.SubjectPublicKeyInfo)
    
    mango_proxydevices_array_string = json.dumps([{"name": device} for device in sorted(list(proxy_devices))])

    udmi_publisher_section = TEMPLATE_MANGO_UDMI_PUBLISHER.substitute(
        mango_publisher_xid=f"PUB_UDMI_{args.publisher}",
        mango_publisher_name=args.publisher,
        mango_proxydevices_array=mango_proxydevices_array_string,
        mango_rsa_privatekey=private_key_pem.decode('utf-8').replace("\n", "\\n"),
        mango_rsa_publickey=public_key_pem.decode('utf-8').replace("\n", "\\n")
    )
    
    udmi_published_points_sections = [
        TEMPLATE_MANGO_PUBLISHED_POINT.substitute(**point) for point in udmi_published_points
    ]

    # Write to files
    with open(output_mango_bacnet_config_filename, 'w') as f_bacnet:
        f_bacnet.write("{\n")
        f_bacnet.write(bacnet_localdevice_section)
        f_bacnet.write(",\n")
        f_bacnet.write('"dataSources": [\n' + ",\n".join(data_source_sections) + "\n],\n")
        f_bacnet.write('"dataPoints": [\n' + ",\n".join(bacnet_data_points_sections) + "\n]\n")
        f_bacnet.write("}")
        print(f"\nCreated {output_mango_bacnet_config_filename}")

    with open(output_mango_udmi_publisher_filename, 'w') as f_udmi:
        f_udmi.write('{\n')
        f_udmi.write(TEMPLATE_MANGO_SYSTEM_SETTINGS.substitute(
            gcp_project_id=args.project,
            gcp_cloud_region=args.region,
            registry_id=args.registry,
            site_id=args.site
        ))
        f_udmi.write('\n  "publishers": [\n' + udmi_publisher_section + '\n],\n')
        f_udmi.write('"publishedPoints": [\n' + ",\n".join(udmi_published_points_sections) + '\n]\n')
        f_udmi.write("}")
        print(f"Created {output_mango_udmi_publisher_filename}")


if __name__ == "__main__":
    main()
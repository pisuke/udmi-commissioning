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
import BAC0
from tabulate import tabulate
import os
import socket
import ipaddress
from string import Template
# from math import isnan
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

TEMPLATE_BACNET_DATASOURCE = """
  "dataSources": [
    {
      "xid": "DS_BACNET",
      "name": "BACNET",
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
    }
  ]"""

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
    "useCovSubscription": true,
    "writePriority": 16
    },
    "eventDetectors": [],
    "plotType": "SPLINE",
    "rollup": "NONE",
    "unit": "",
    "simplifyType": "NONE",
    "chartColour": "",
    "data": null,
    "dataSourceXid": "DS_BACNET",
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

TEMPLATE_MANGO_UDMI_PUBLISHER = Template("""
{
    "xid": "${mango_publisher_xid}",
    "name": "${mango_publisher_name}",
    "enabled": true,
    "type": "UDMI",
    "snapshotSendPeriodType": "MINUTES",
    "publishType": "ALL",
    "alarmLevels": {
    "POINT_DISABLED_EVENT": "URGENT",
    "QUEUE_SIZE_WARNING_EVENT": "URGENT"
    },
    "gateway": true,
    "proxyDevices": ${mango_proxydevices_array},
    "rsaPrivateKey": "${mango_rsa_privatekey}",
    "rsaPublicKey": "${mango_rsa_publickey}",
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

    title = """
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


def get_sheets_from_file(file_path):
    """
    Loads the content of an XLSX or ODS file and returns a list of sheet names.

    Args:
      file_path: Path to the XLSX or ODS file.

    Returns:
      A list of sheet names in the file.
    """
    try:
        # Attempt to read the file using pandas
        excel_file = pd.ExcelFile(file_path)
        return excel_file.sheet_names

    except Exception as e:
        print(f"Error reading file: {e}")
        return []

def get_sheet_data(file_path, sheet_name):
  """
  Loads the content of a specific sheet from an XLSX or ODS file.

  Args:
    file_path: Path to the XLSX or ODS file.
    sheet_name: Name of the sheet to read.

  Returns:
    A pandas DataFrame containing the data from the specified sheet, 
    or None if the sheet is not found.
  """
  try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df
  except FileNotFoundError:
    print(f"Error: File not found: {file_path}")
    return None
  except KeyError:
    print(f"Error: Sheet '{sheet_name}' not found in the file.")
    return None
  except Exception as e:
    print(f"An unexpected error occurred: {e}")
    return None

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
    ip_address = ipaddress.ip_interface(local_ip)
    return str(ip_address.network.broadcast_address)
  except Exception as e:
    print(f"Error getting broadcast address: {e}")
    return None

def isNaN(num):
    return num != num
  
def add_to_list_if_not_exists(my_list, new_entry):
  """
  Adds a new entry to the list if it doesn't already exist.

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
    
    

    args = parser.parse_args()
    
    bacnet_localdevice_id = args.localdevice
    broadcast_address = args.broadcast 

    if args.verbose:
        print("program arguments:")
        print(args)

    if args.input and args.output and os.path.exists(args.input):
        
        sheets = get_sheets_from_file(args.input)
        
        if sheets:
            print(f"Sheets found in {args.input}:")
        for sheet in sheets:
            print(f"- {sheet}")
            
        print()
            
        if "devices" in sheets:
            devices_data = get_sheet_data(args.input, "devices")
            if devices_data is not None:
                print(f"Data from sheet 'devices':")
                print(devices_data)
                
        print()
        
        output_mango_bacnet_config_filename = f"{args.output}_bacnet_config.json"
        output_mango_udmi_publisher_filename = f"{args.output}_udmi_publisher.json"
        
        with open(output_mango_bacnet_config_filename, 'w') as output_mango_bacnet_config_file:
            # broadcast_address = get_broadcast_address()
            # if broadcast_address:
            #     print(f"Broadcast Address: {broadcast_address}")
            output_mango_bacnet_config_file.write("{\n")
            output_mango_bacnet_config_file.write(TEMPLATE_BACNET_LOCALDEVICE.substitute(broadcast_address=broadcast_address, bacnet_localdevice_id=bacnet_localdevice_id))
            output_mango_bacnet_config_file.write(",\n")
            output_mango_bacnet_config_file.write(TEMPLATE_BACNET_DATASOURCE)
            output_mango_bacnet_config_file.write(',\n"dataPoints": [\n')
            
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
            
            proxy_devices = []
            
            with open(output_mango_udmi_publisher_filename, 'w') as output_mango_udmi_publisher_file:
              
                key = rsa.generate_private_key(
                    backend=crypto_default_backend(),
                    public_exponent=65537,
                    key_size=2048
                )

                private_key = key.private_bytes(
                    crypto_serialization.Encoding.PEM,
                    crypto_serialization.PrivateFormat.TraditionalOpenSSL,
                    crypto_serialization.NoEncryption()
                )

                public_key = key.public_key().public_bytes(
                    crypto_serialization.Encoding.PEM,
                    crypto_serialization.PublicFormat.PKCS1
                )
              
                output_mango_udmi_publisher_file.write('{\n  "publishers": [\n')
              
                # point_data_type, 
                # point_object_type, 
                # point_remote_object_instance_number
                # point_remote_device_instance_number, 
                # bacnet_device_name, 
                # bacnet_point_name, 
                # bacnet_point_property_name, 
                # bacnet_point_description, 
                # mango_point_xid
                
                # get total number of points to be exported and build the proxy devices list
                points_to_be_exported = 0
                mango_proxydevices_array_string = "["
                
                for index, device in devices_data.iterrows():
                    
                    points_data = get_sheet_data(args.input, device['device_name'])
                    
                    for index, point in points_data.iterrows():
                      cloud_device_id = point['cloud_device_id']  
                      cloud_point_name = point['cloud_point_name']
                      if cloud_device_id and cloud_point_name and not isNaN(cloud_device_id) and not isNaN(cloud_point_name):
                          proxy_devices = add_to_list_if_not_exists(proxy_devices, cloud_device_id)
                          points_to_be_exported += 1
                          
                print("Proxy devices: ",proxy_devices)
                          
                # build the proxy devices list string
                pd = 0
                for proxy_device in proxy_devices:
                  pd += 1
                  
                  if pd < len(proxy_devices):
                    mango_proxydevices_array_string += '{"name": "%s"},' % proxy_device
                  else:
                    mango_proxydevices_array_string += '{"name": "%s"}' % proxy_device
                    
                mango_proxydevices_array_string += "]"
                                       
                proxy_devices_to_be_exported = len(proxy_devices)
                mango_publisher_xid = "PUB_UDMI_CGW-1"
                mango_publisher_name = "CGW-1"
                          
                output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_UDMI_PUBLISHER.substitute(
                  mango_publisher_xid=mango_publisher_xid,
                  mango_publisher_name=mango_publisher_name,
                  mango_proxydevices_array=mango_proxydevices_array_string,
                  mango_rsa_privatekey=private_key,
                  mango_rsa_publickey=public_key
                ))
                
                output_mango_udmi_publisher_file.write('],"publishedPoints": [')
                  
                i = 0
                
                for index, device in devices_data.iterrows():
                    # pi = 0
                    if device['device_name'] != "BAC0":
                        print("\n",device['device_name'], device['device_vendor'], device['device_model'],device['device_id'])
                        points_data = get_sheet_data(args.input, device['device_name'])
                        # di += 1
                        # print(i,len(devices_data))
                        for index, point in points_data.iterrows():
                            
                            # print(i,len(points_data))
                          
                            cloud_device_id = point['cloud_device_id']
                            # proxy_devices = add_to_list_if_not_exists(proxy_devices, cloud_device_id)
                            cloud_point_name = point['cloud_point_name']
                            
                            if cloud_device_id and cloud_point_name and not isNaN(cloud_device_id) and not isNaN(cloud_point_name):
                              
                                i += 1

                                point_data_type = "NUMERIC"
                                
                                mango_point_name = cloud_point_name
                                mango_device_id = cloud_device_id
                                
                                point_remote_object_instance_number = point['object'].split(":")[1]
                                point_remote_device_instance_number = device['device_id']
                                point_object_type = OBJECT_TYPE[point['object'].split(":")[0]]

                                bacnet_point_property_name = point_object_type
                                if isNaN(point['description']):
                                  bacnet_point_description = ""
                                else:
                                  bacnet_point_description = point['description']
                                # DP_2802621_ANALOG_VALUE_20
                                mango_point_xid = f"DP_{device['device_id']}_{point_object_type}_{point_remote_object_instance_number}"
                                bacnet_point_name = point['point_name']
                                bacnet_device_name = point['device_name']
                                print(mango_device_id, bacnet_device_name, cloud_point_name, bacnet_point_name, mango_point_xid)
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
                                                      mango_point_xid=mango_point_xid
                                                      )
                                          )
                              
                                mango_publisher_xid = "PUB_UDMI_CGW-1"
                                output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_PUBLISHED_POINT.substitute(
                                        point_name=mango_point_name,
                                        point_xid=mango_point_xid,
                                        device_name=mango_device_id,
                                        mango_publisher_xid=mango_publisher_xid
                                ))
                                
                                # if it is not the last point, add a comma for each point
                                if i < points_to_be_exported:
                                  output_mango_bacnet_config_file.write(',\n')
                                  output_mango_udmi_publisher_file.write(',\n')
                        
                output_mango_bacnet_config_file.write("]\n}")
                output_mango_udmi_publisher_file.write("]\n}")
                          
            
            
    else:
        print("Please provide both input and output filename prefix")


if __name__ == "__main__":
    main()

"""
{
      "name": "zone_temperature",
      "enabled": true,
      "loggingType": "INTERVAL",
      "intervalLoggingPeriodType": "MINUTES",
      "intervalLoggingType": "AVERAGE",
      "purgeType": "YEARS",
      "pointLocator": {
        "dataType": "NUMERIC",
        "objectType": "ANALOG_INPUT",
        "propertyIdentifier": "present-value",
        "additive": 0,
        "multiplier": 1,
        "objectInstanceNumber": 1,
        "remoteDeviceInstanceNumber": 2803104,
        "settable": false,
        "useCovSubscription": true,
        "writePriority": 16
      },
      "eventDetectors": [],
      "plotType": "SPLINE",
      "rollup": "NONE",
      "unit": "",
      "simplifyType": "NONE",
      "chartColour": "",
      "data": null,
      "dataSourceXid": "DS_BACNET",
      "defaultCacheSize": 1,
      "deviceName": "HTR-1",
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
        "BACnetDeviceName": "HTR-1",
        "BACnetObjectName": "zone_temp_1",
        "BACnetPropertyName": "ANALOG_INPUT",
        "BACnetObjectDescription": "Zone Temperature"
      },
      "textRenderer": {
        "type": "ANALOG",
        "useUnitAsSuffix": false,
        "suffix": "",
        "format": "0.00"
      },
      "tolerance": 0,
      "xid": "DP_2803104_ANALOG_INPUT_1"
    },
"""


"""
analogInput:1
analogOutput:1
analogValue:1
binaryInput:3
binaryOutput:4
binaryValue:1
multiStateInput:9697
multiStateOutput:9697
multiStateValue:1
loop:1

"""

"""
Datapoint Type / BACnet Object Type	  Object Type ID	  Brief Description
BACnet_AI	                            0	                Analog input. Defines a standard object whose properties represent the externally visible characteristics of an analog input.
BACnet_AO	                            1	Analog output. Defines a standard object whose properties represent the externally visible characteristics of an analog output.
BACnet_AV	                            2	Analog value. Defines a standard object whose properties represent the externally visible characteristics of an analog value.
BACnet_BI	                            3	Binary input. Defines a standard object whose properties represent the externally visible characteristics of a binary input.
BACnet_BO	                            4	Binary output. Defines a standard object whose properties represent the externally visible characteristics of a binary output.
BACnet_BV	5	Binary value. Defines a standard object whose properties represent the externally visible characteristics of a binary value.
BACnet_MSI	13	Multi-state input. Defines a standard object whose Present_Value (present value) can take integer values.
BACnet_MSO	14	Multi-state output. Defines a standard object whose output is an integer value.
BACnet_MSV	19	Multi-state value. Defines a standard object whose properties represent the externally visible characteristics of a multistage value.
BACnet_Accumulator	23	Addition of impulse data of measurement devices over the time. Used for balancing, statement and energy performance management (for interval mass counter see Pulse Converter).
BACnet_Calendar	6	Calendar. Defines a standard object that is used to define a list of calendar entries (date list).
BACnet_Schedule	17	Time plan. Defines a standard object that is used to define a periodic timeplan, also with optional exceptions on arbitrary days or on arbitrary dates, which can recur within a certain period of time.
BACnet_NOC	15	Alarm class. Defines a standard object that contains the necessary information for the distribution of event alarms with BACnet systems.
BACnet_Program	16	Object for program controlling in a BACnet device (e.g. load and start).
BACnet_PulseConverter	24	Object for impulse conversion for mass counting in defined time intervals.
BACnet_TrendLog	20	Trend Log. Archives a property of a referenced object and, when predefined conditions are met, saves the value of the property and a timestamp in an internal buffer for subsequent retrieval.
BACnet_Device	8	BACnet device. Defines a standard object whose properties represent the externally visible characteristics of a BACnet device.
"""
#!/usr/bin/env python3

"""
  sheet2mangojson.py

  Converts a spreadsheet including BACnet information from bacnet-scan
  to a Mango JSON input file.

  [Usage] 
  python3 sheet2mangojson.py
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
# import BAC0 
from tabulate import tabulate
import os
import socket
import ipaddress
from string import Template
# from math import isnan
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend

# --- Tkinter Imports for the GUI ---
import tkinter as tk
from tkinter import filedialog, messagebox
# --- End Tkinter Imports ---


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

def get_sheets_from_file(file_path):
    """
    Loads the content of an XLSX or ODS file and returns a list of sheet names.
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        return excel_file.sheet_names
    except Exception as e:
        print(f"Error reading file: {e}")
        return []

def get_sheet_data(file_path, sheet_name):
  """
  Loads the content of a specific sheet from an XLSX or ODS file.
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
  
def add_to_list_if_not_exists(my_list, new_entry):
  """
  Adds a new entry to the list if it doesn't already exist.
  """
  if new_entry not in my_list:
    my_list.append(new_entry)
  return my_list

# --- NEW GUI CLASS ---

class ConfigGUI:
    def __init__(self, master):
        self.master = master
        master.title("sheet2mangojson Config")
        
        self.fields = {}
        self.defaults = {
            'input_file': '',
            'output_prefix': 'mango_output', # Default output prefix
            'localdevice': '98765',
            'broadcast': '255.255.255.255',
            'publisher': 'CGW-1',
            'project': 'gcp-project-name',
            'region': 'us-central1',
            'registry': 'ZZ-ABC-DEF',
            'site': 'ZZ-ABC-DEF',
            'unique': tk.BooleanVar(value=False)
        }
        
        # Define fields and their labels
        # Format: (Label Text, Variable Name, Is Required, [Optional: Field Type])
        row_config = [
            ("Input Spreadsheet", 'input_file', True),
            ("Output File Prefix", 'output_prefix', True),
            ("Unique DS per Device", 'unique', False, 'checkbox'),
            ("BACnet Local Device ID (-l)", 'localdevice', False),
            ("BACnet Broadcast Address (-b)", 'broadcast', False),
            ("UDMI Publisher Name (-p)", 'publisher', False),
            ("GCP Project ID (-j)", 'project', False),
            ("GCP Region (-g)", 'region', False),
            ("IoT Core Registry ID (-r)", 'registry', False),
            ("IoT Core Site Name (-s)", 'site', False),
        ]
        
        # Build the form
        r = 0
        
        # FIX APPLIED HERE: Ensure the list comprehension correctly normalizes 3-item 
        # tuples to 4 items by appending 'text', and uses 4-item tuples as is.
        processed_row_config = [t + ('text',) if len(t) == 3 else t for t in row_config]

        for label_text, var_name, required, field_type in processed_row_config:
            
            tk.Label(master, text=label_text + ( " *" if required else "") + ":").grid(row=r, column=0, sticky='w', padx=5, pady=2)
            
            if field_type == 'text':
                var = tk.StringVar(value=self.defaults.get(var_name, ''))
                entry = tk.Entry(master, textvariable=var, width=50)
                entry.grid(row=r, column=1, padx=5, pady=2)
                self.fields[var_name] = var
                
                if var_name == 'input_file':
                    tk.Button(master, text="Browse", command=self.browse_file).grid(row=r, column=2, padx=5, pady=2)
            
            elif field_type == 'checkbox':
                var = self.defaults.get(var_name)
                chk = tk.Checkbutton(master, variable=var)
                chk.grid(row=r, column=1, sticky='w', padx=5, pady=2)
                self.fields[var_name] = var

            r += 1

        # Run Button
        tk.Button(master, text="Run Script", command=self.validate_and_run, bg='green', fg='white').grid(row=r, column=1, columnspan=2, pady=10)

    def browse_file(self):
        filename = filedialog.askopenfilename(
            defaultextension=".xlsx",
            filetypes=[("Spreadsheet files", "*.xlsx *.ods"), ("All files", "*.*")]
        )
        if filename:
            self.fields['input_file'].set(filename)

    def validate_and_run(self):
        # Retrieve all input values
        input_file = self.fields['input_file'].get().strip()
        output_prefix = self.fields['output_prefix'].get().strip()
        
        # Validation checks
        if not input_file:
            messagebox.showerror("Validation Error", "Input Spreadsheet file is required.")
            return

        if not os.path.exists(input_file):
            messagebox.showerror("Validation Error", f"Input file not found: {input_file}")
            return
            
        if not output_prefix:
            messagebox.showerror("Validation Error", "Output File Prefix is required.")
            return

        # Gather all data into a dictionary 
        data = {k: v.get() if isinstance(v, (tk.StringVar, tk.BooleanVar)) else v for k, v in self.fields.items()}

        # Close the GUI window
        self.master.destroy()

        # Run the core logic with the gathered data
        run_core_logic(data)

# --- CORE LOGIC FUNCTION ---

def run_core_logic(data):
    """
    Executes the main logic of the script using parameters supplied by the GUI.
    """
    show_title()

    # Map the GUI data keys to the original variable names
    bacnet_localdevice_id = data['localdevice']
    broadcast_address = data['broadcast'] 
    publisher_name = data['publisher']
    gcp_project_id = data['project']
    gcp_cloud_region = data['region']
    registry_id = data['registry']
    site_id = data['site']
    
    input_file = data['input_file']
    output_prefix = data['output_prefix']
    unique_ds = data['unique'] # This is a boolean

    print(f"Running script with the following parameters:")
    pprint(data)
    print("\n" + "="*50 + "\n")


    if input_file and output_prefix and os.path.exists(input_file):
        
        sheets = get_sheets_from_file(input_file)
        
        if sheets:
            print(f"Sheets found in {input_file}:")
        for sheet in sheets:
            print(f"- {sheet}")
            
        print()
            
        devices_data = None
        if "devices" in sheets:
            devices_data = get_sheet_data(input_file, "devices")
            if devices_data is not None:
                print(f"Data from sheet 'devices':")
                print(tabulate(devices_data.head(), headers='keys', tablefmt='psql')) 
                
        print()
        
        if devices_data is None:
            print("ERROR: 'devices' sheet not found or empty. Exiting.")
            return

        output_mango_bacnet_config_filename = f"{output_prefix}_bacnet_config.json"
        output_mango_udmi_publisher_filename = f"{output_prefix}_udmi_publisher.json"
        
        with open(output_mango_bacnet_config_filename, 'w') as output_mango_bacnet_config_file:
            output_mango_bacnet_config_file.write("{\n")
            output_mango_bacnet_config_file.write(TEMPLATE_BACNET_LOCALDEVICE.substitute(broadcast_address=broadcast_address, bacnet_localdevice_id=bacnet_localdevice_id))
            output_mango_bacnet_config_file.write(",\n")
            
            # --- Start of unique data source logic ---
            if unique_ds:
                # Get unique sanitized device names to create separate data sources
                sanitized_device_names = [device['sanitized_device_name'] for index, device in devices_data.iterrows() if device['sanitized_device_name'] != "BAC0" and not isNaN(device['sanitized_device_name'])]
                unique_device_names = sorted(list(set(sanitized_device_names)))
                
                output_mango_bacnet_config_file.write('"dataSources": [\n')
                for i, device_name in enumerate(unique_device_names):
                    data_source_xid = f"DS_BACNET_{device_name}"
                    data_source_name = f"BACNET_{device_name}"
                    output_mango_bacnet_config_file.write(TEMPLATE_BACNET_DATASOURCE.substitute(data_source_xid=data_source_xid, data_source_name=data_source_name))
                    if i < len(unique_device_names) - 1:
                        output_mango_bacnet_config_file.write(",\n")
                output_mango_bacnet_config_file.write("\n],")

            else:
                # Default behavior: single data source
                data_source_xid = "DS_BACNET"
                data_source_name = "BACNET"
                output_mango_bacnet_config_file.write('"dataSources": [\n')
                output_mango_bacnet_config_file.write(TEMPLATE_BACNET_DATASOURCE.substitute(data_source_xid=data_source_xid, data_source_name=data_source_name))
                output_mango_bacnet_config_file.write('\n],')
            
            output_mango_bacnet_config_file.write('\n"dataPoints": [\n')
            # --- End of unique data source logic ---

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
                
                output_mango_udmi_publisher_file.write('{\n')
                
                output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_SYSTEM_SETTINGS.substitute(
                    gcp_project_id=gcp_project_id,
                    gcp_cloud_region=gcp_cloud_region,
                    registry_id=registry_id,
                    site_id=site_id
                  )
                )
              
                output_mango_udmi_publisher_file.write('\n  "publishers": [\n')
              
                points_to_be_exported = 0
                
                # First pass to count points and determine all proxy devices
                print("\nCounting points and identifying proxy devices...")
                for index, device in devices_data.iterrows():
                    points_data = get_sheet_data(input_file, device['sanitized_device_name'])
                    if points_data is not None:
                      for index, point in points_data.iterrows():
                        cloud_device_id = point.get('cloud_device_id')  
                        cloud_point_name = point.get('cloud_point_name')
                        if cloud_device_id and cloud_point_name and not isNaN(cloud_device_id) and not isNaN(cloud_point_name):
                            proxy_devices = add_to_list_if_not_exists(proxy_devices, cloud_device_id)
                            points_to_be_exported += 1
                          
                print(f"Total points to export: {points_to_be_exported}")
                print("Proxy devices: ",proxy_devices)
                          
                mango_proxydevices_array_string = "["
                pd = 0
                for proxy_device in proxy_devices:
                  pd += 1
                  if pd < len(proxy_devices):
                    mango_proxydevices_array_string += '{"name": "%s"},' % proxy_device
                  else:
                    mango_proxydevices_array_string += '{"name": "%s"}' % proxy_device
                    
                mango_proxydevices_array_string += "]"
                                       
                mango_publisher_xid = f"PUB_UDMI_BACNET_{publisher_name}"

                mango_publisher_name = publisher_name
                          
                output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_UDMI_PUBLISHER.substitute(
                  mango_publisher_xid=mango_publisher_xid,
                  mango_publisher_name=mango_publisher_name,
                  mango_proxydevices_array=mango_proxydevices_array_string,
                  mango_rsa_privatekey=private_key.decode('utf-8').replace("\n", "\\n"),
                  mango_rsa_publickey=public_key.decode('utf-8').replace("\n", "\\n")
                ))
                
                output_mango_udmi_publisher_file.write('],"publishedPoints": [')
                  
                i = 0
                
                # Second pass to generate JSON content
                print("\nGenerating JSON files...")
                for index, device in devices_data.iterrows():
                    sanitized_device_name = device.get('sanitized_device_name')
                    device_id = device.get('device_id')

                    if sanitized_device_name != "BAC0" and not isNaN(sanitized_device_name):
                        print(f"\nProcessing device: {sanitized_device_name} (ID: {device_id})")
                        points_data = get_sheet_data(input_file, sanitized_device_name)
                        
                        if points_data is not None:
                          # Determine the data source XID based on the --unique flag
                          if unique_ds:
                              current_data_source_xid = f"DS_BACNET_{sanitized_device_name}"
                          else:
                              current_data_source_xid = "DS_BACNET"
                        
                          for index, point in points_data.iterrows():
                              cloud_device_id = point.get('cloud_device_id')
                              cloud_point_name = point.get('cloud_point_name')
                              
                              if cloud_device_id and cloud_point_name and not isNaN(cloud_device_id) and not isNaN(cloud_point_name):
                                
                                  i += 1

                                  point_data_type = "NUMERIC"
                                  
                                  mango_point_name = cloud_point_name
                                  mango_device_id = cloud_device_id
                                  
                                  point_obj_str = str(point.get('object', '')).split(":")
                                  point_remote_object_instance_number = point_obj_str[1] if len(point_obj_str) > 1 else '0'
                                  point_remote_device_instance_number = device_id
                                  point_object_type = OBJECT_TYPE.get(point_obj_str[0], 'ANALOG_VALUE')

                                  bacnet_point_property_name = point_object_type
                                  bacnet_point_description = point.get('description', '')
                                  if isNaN(bacnet_point_description):
                                    bacnet_point_description = ""
                                  
                                  mango_point_xid = f"DP_{device_id}_{point_object_type}_{point_remote_object_instance_number}"
                                  bacnet_point_name = point.get('point_name', '')
                                  bacnet_device_name = point.get('device_name', '')
                                  
                                  print(f"  -> Point: {mango_point_name} (XID: {mango_point_xid})")
                                  
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
                                
                                  # Use the dynamically generated publisher XID
                                  output_mango_udmi_publisher_file.write(TEMPLATE_MANGO_PUBLISHED_POINT.substitute(
                                          point_name=mango_point_name,
                                          point_xid=mango_point_xid,
                                          device_name=mango_device_id,
                                          mango_publisher_xid=mango_publisher_xid
                                  ))
                                  
                                  if i < points_to_be_exported:
                                    output_mango_bacnet_config_file.write(',\n')
                                    output_mango_udmi_publisher_file.write(',\n')
                        
                output_mango_bacnet_config_file.write("]\n}")
                output_mango_udmi_publisher_file.write("]\n}")
                
                print("\n" + "="*50)
                print(f"Successfully created files:")
                print(f"- {output_mango_bacnet_config_filename}")
                print(f"- {output_mango_udmi_publisher_filename}")
                print("="*50)
                          
    else:
        print("Script finished without processing due to file errors or user cancellation.")


# --- MAIN EXECUTION BLOCK ---

def main():
    # Launch the GUI
    root = tk.Tk()
    app = ConfigGUI(root)
    # The GUI will block here until the user closes it or clicks 'Run Script'
    root.mainloop()

if __name__ == "__main__":
    main()
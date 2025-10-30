#!/usr/bin/env python3

"""
  sheet2mangojson.py

  Converts a spreadsheet including BACnet information from bacnet-scan
  to a Mango JSON input file.

  [Usage] 
  1. Interactive GUI mode: python3 sheet2mangojson.py
  2. CLI mode: python3 sheet2mangojson.py -i <input_file> -o <output_prefix> [options]
"""

__author__ = "Francesco Anselmo"
__copyright__ = "Copyright 2025"
__credits__ = ["Francesco Anselmo"]
__license__ = "MIT"
__version__ = "0.2"
__maintainer__ = "Francesco Anselmo"
__email__ = "francesco.anselmo@gmail.com"
__status__ = "Dev"

from pprint import pprint
import argparse 
import pandas as pd
from tabulate import tabulate
import os
import socket
import ipaddress
import uuid 
import sys # Required for checking command-line arguments
from string import Template
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend
import tkinter as tk
from tkinter import filedialog, messagebox


# ----------------------------------------------------------------------
# --- TEMPLATE DEFINITIONS (V5.4.* - Latest Version) ---
# ----------------------------------------------------------------------

TEMPLATE_BACNET_LOCALDEVICE_V5_4 = Template('''
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
     "retries": ${retries},
     "retryCount": 1,
     "reuseAddress": false,
     "segTimeout": ${seg_timeout},
     "segWindow": 5,
     "subnet": 24,
     "thisStation": 0,
     "timeout": ${timeout},
     "type": "ip",
     "usageTimeout": 20,
     "useRealtime": false
   }
]''')

TEMPLATE_BACNET_DATASOURCE_V5_4 = Template("""
    {
      "xid": "${data_source_xid}",
      "name": "${data_source_name}",
      "enabled": ${data_source_enabled},
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

TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_4 = Template("""
  "systemSettings": {
    "udmi.config": "[{\\"xid\\": \\"${udmi_config_xid}\\", \\"iotProvider\\": \\"GBOS\\", \\"projectId\\": \\"${gcp_project_id}\\", \\"cloudRegion\\": \\"${gcp_cloud_region}\\", \\"registryId\\": \\"${registry_id}\\", \\"site\\": \\"${site_id}\\", \\"hostname\\": \\"${hostname}\\"}]"
  },
""")

TEMPLATE_MANGO_UDMI_PUBLISHER_V5_4 = Template("""
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
    "caCertificate": null,
    "clientCertificate": null,
    "deviceId": "${publisher_device_id}",
    "gateway": true,
    "proxyDevices": ${mango_proxydevices_array},
    "privateKey": "${mango_rsa_privatekey}",
    "publicKey": "${mango_rsa_publickey}",
    "udmiConfigXid": "${udmi_config_xid}",
    "cacheDiscardSize": 50000,
    "cacheWarningSize": 10000,
    "publishAttributeChanges": false,
    "publishPointEvents": false,
    "sendSnapshot": false,
    "snapshotSendPeriods": 5
}""")

# ----------------------------------------------------------------------
# --- TEMPLATE DEFINITIONS (V5.3.* - Older Version) ---
# ----------------------------------------------------------------------

# NOTE: Modified to accept dynamic retries, timeout, and seg_timeout
TEMPLATE_BACNET_LOCALDEVICE_V5_3 = Template('''
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
     "retries": ${retries},
     "retryCount": 1,
     "reuseAddress": false,
     "segTimeout": ${seg_timeout},
     "segWindow": 5,
     "subnet": 24,
     "thisStation": 0,
     "timeout": ${timeout},
     "type": "ip",
     "usageTimeout": 20,
     "useRealtime": false
   }
]''')

# NOTE: Modified to accept dynamic enabled status
TEMPLATE_BACNET_DATASOURCE_V5_3 = Template("""
    {
      "xid": "${data_source_xid}",
      "name": "${data_source_name}",
      "enabled": ${data_source_enabled},
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

# NOTE: Uses the simple, non-nested string structure and omits XID/hostname fields
TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_3 = Template("""
  "systemSettings": {
    "udmi.config": "{\\"iotProvider\\": \\"GBOS\\", \\"projectId\\": \\"${gcp_project_id}\\", \\"cloudRegion\\": \\"${gcp_cloud_region}\\", \\"registryId\\": \\"${registry_id}\\", \\"site\\": \\"${site_id}\\"}"
  },
""")

# NOTE: Omits udmiConfigXid, deviceId, caCertificate, clientCertificate
TEMPLATE_MANGO_UDMI_PUBLISHER_V5_3 = Template("""
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

# Common Templates (Unchanged)
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

TEMPLATE_MANGO_PUBLISHED_POINT = Template('''
    {
      "name": "${point_name}",
      "enabled": true,
      "dataPointXid": "${point_xid}",
      "deviceName": "${device_name}",
      "publisherXid": "${mango_publisher_xid}"
    }''')

# --- UTILITY FUNCTIONS ---
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

def read_all_sheets(file_path):
    """
    Reads the entire Excel/ODS file into a dictionary of DataFrames at once
    for maximum I/O efficiency.
    """
    try:
        # Use sheet_name=None to read all sheets into a dictionary: {sheet_name: DataFrame}
        return pd.read_excel(file_path, sheet_name=None)
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during file read: {e}")
        return None

def isNaN(num):
    """Checks for NaN values."""
    try:
        # Use pandas/numpy for efficient NaN checking
        import numpy as np
        return pd.isna(num) or np.isnan(num)
    except Exception:
        return num != num

def generate_udmi_config_xid(registry_id, project_id):
    """
    Generates a non-random, meaningful XID for the UDMI config entry.
    Format: UDMI-CONFIG-PROJECTID-REGISTRYID
    """
    
    # Use IDs directly, join with hyphens, and ensure upper case.
    # Replace any leftover underscores/dots with hyphens.
    xid = f"UDMI-CONFIG-{project_id}-{registry_id}"
    return xid.upper().replace('_', '-').replace('.', '-')

# --- GUI CLASS (Unchanged) ---
class ConfigGUI:
    # ... (GUI Class definition remains the same)
    def __init__(self, master):
        self.master = master
        master.title("sheet2mangojson - configuration")
        
        self.fields = {}
        
        self.defaults = {
            'input_file': '',
            'output_prefix': 'ZZ-ABC-DEF_round-1',
            'localdevice': '98777',
            'broadcast': '255.255.255.255',
            'publisher': 'CGWV-1',
            'project': 'bos-platform-prod',
            'region': 'us-central1',
            'registry': 'ZZ-ABC-DEF',
            'site': 'ZZ-ABC-DEF',
            'hostname': 'mqtt.bos.goog', 
            'unique': tk.BooleanVar(value=True),
            'timeout': '30000',
            'retries': '0',
            'seg_timeout': '10000',
            'ds_enabled': tk.BooleanVar(value=True), 
            'debug_mode': tk.BooleanVar(value=False), 
            'udmi_version': tk.StringVar(value='5.4.*'), 
        }
        
        # --- Define Configuration Sections and Fields ---
        self.config_sections = [
            ("1. File & General Configuration", [
                ("Input Spreadsheet", 'input_file', True),
                ("Output File Prefix", 'output_prefix', True),
                ("UDMI Driver Version", 'udmi_version', ['5.4.*', '5.3.*'], 'dropdown'),
                ("Enable Verbose Output", 'debug_mode', False, 'checkbox'),
            ]),
            ("2. BACnet Device & Data Source Configuration", [
                ("BACnet Local Device ID", 'localdevice', False),
                ("BACnet Broadcast Address", 'broadcast', False),
                ("BACnet Timeout (ms)", 'timeout', False),
                ("BACnet Retries", 'retries', False),
                ("BACnet Segment Timeout (ms)", 'seg_timeout', False),
                ("Unique Data Source per Device", 'unique', False, 'checkbox'),
                ("Data Source Enabled by Default", 'ds_enabled', False, 'checkbox'),
            ]),
            ("3. UDMI Publisher Configuration", [
                ("UDMI Publisher Name", 'publisher', False),
                ("GCP Project ID", 'project', False),
                ("GCP Region", 'region', False),
                ("IoT Core Registry ID", 'registry', False),
                ("IoT Core Site Name", 'site', False),
                ("UDMI Hostname", 'hostname', False), 
            ])
        ]

        # Create the main container frame
        main_frame = tk.Frame(master)
        main_frame.pack(padx=10, pady=10, fill='both', expand=True)

        frame_row = 0
        for section_title, fields_list in self.config_sections:
            # Create a labeled frame for the section
            label_frame = tk.LabelFrame(main_frame, text=section_title, padx=10, pady=10)
            label_frame.grid(row=frame_row, column=0, sticky="ew", pady=10)
            
            r = 0
            # Process fields within the current labeled frame
            for item in fields_list:
                
                # Check for dropdown field type
                if len(item) == 4 and item[3] == 'dropdown':
                    label_text, var_name, options, _ = item
                    var = self.defaults.get(var_name)
                    
                    tk.Label(label_frame, text=f"{label_text}:").grid(row=r, column=0, sticky='w', padx=5, pady=2)
                    
                    option_menu = tk.OptionMenu(label_frame, var, *options)
                    option_menu.grid(row=r, column=1, padx=5, pady=2, sticky='ew')
                    self.fields[var_name] = var
                    
                else: # Handles 'text' (3-tuple) and 'checkbox' (4-tuple)
                    field_data = item + ('text',) if len(item) == 3 else item
                    label_text, var_name, required, field_type = field_data
                    
                    tk.Label(label_frame, text=label_text + ( " *" if required else "") + ":").grid(row=r, column=0, sticky='w', padx=5, pady=2)
                    
                    if field_type == 'text':
                        var = tk.StringVar(value=self.defaults.get(var_name, ''))
                        entry = tk.Entry(label_frame, textvariable=var, width=50)
                        entry.grid(row=r, column=1, padx=5, pady=2, sticky='ew')
                        self.fields[var_name] = var
                        
                        if var_name == 'input_file':
                            tk.Button(label_frame, text="Browse", command=self.browse_file).grid(row=r, column=2, padx=5, pady=2)
                    
                    elif field_type == 'checkbox':
                        var = self.defaults.get(var_name) 
                        chk = tk.Checkbutton(label_frame, variable=var)
                        chk.grid(row=r, column=1, sticky='w', padx=5, pady=2)
                        self.fields[var_name] = var

                r += 1
            
            label_frame.grid_columnconfigure(1, weight=1)
            frame_row += 1

        tk.Button(main_frame, text="Run Script", command=self.validate_and_run, bg='green', fg='white').grid(row=frame_row, column=0, pady=10, sticky='ew')

    def browse_file(self):
        filename = filedialog.askopenfilename(
            defaultextension=".xlsx",
            filetypes=[("Spreadsheet files", "*.xlsx *.ods"), ("All files", "*.*")]
        )
        if filename:
            self.fields['input_file'].set(filename)

    def validate_and_run(self):
        input_file = self.fields['input_file'].get().strip()
        output_prefix = self.fields['output_prefix'].get().strip()
        
        # --- Validation Checks (Minimal) ---
        if not input_file:
            messagebox.showerror("Validation Error", "Input Spreadsheet file is required.")
            return

        if not os.path.exists(input_file):
            messagebox.showerror("Validation Error", f"Input file not found: {input_file}")
            return
            
        if not output_prefix:
            messagebox.showerror("Validation Error", "Output File Prefix is required.")
            return
        # --- End Validation Checks ---

        data = {k: v.get() if isinstance(v, (tk.StringVar, tk.BooleanVar)) else v for k, v in self.fields.items()}

        self.master.destroy()

        run_core_logic(data)

# --- Argument Mapping Function ---
def map_args_to_data(args, defaults):
    """
    Converts argparse namespace object into the standardized data dictionary
    required by run_core_logic.
    """
    data = {
        'input_file': args.input,
        'output_prefix': args.output,
        
        'localdevice': str(args.localdevice),
        'broadcast': args.broadcast,
        'publisher': args.publisher,
        'project': args.project,
        'region': args.region,
        'registry': args.registry,
        'site': args.site,
        'hostname': args.hostname,
        
        'unique': args.unique, # Boolean from store_true
        
        # New customizable BACnet fields
        'timeout': str(args.timeout),
        'retries': str(args.retries),
        'seg_timeout': str(args.segtimeout),
        
        # Boolean mapping: 'True'/'False' string in CLI to Python bool/string in data dict
        'ds_enabled': args.ds_enabled.lower() == 'true',
        
        # Verbosity/Debug mapping
        'debug_mode': args.verbose,
        
        # UDMI Version mapping
        'udmi_version': args.udmi_version,
    }
    return data

# --- CORE LOGIC FUNCTION (Optimized and Updated) ---

def run_core_logic(data):
    """
    Executes the main logic of the script using parameters supplied by the GUI/CLI.
    """
    show_title()

    # Map the standardized data dictionary keys
    bacnet_localdevice_id = data['localdevice']
    broadcast_address = data['broadcast'] 
    publisher_name = data['publisher']
    gcp_project_id = data['project']
    gcp_cloud_region = data['region'] 
    registry_id = data['registry']
    site_id = data['site']
    hostname = data['hostname'] 
    input_file = data['input_file']
    output_prefix = data['output_prefix']
    unique_ds = data['unique']
    
    # BACnet Local Device parameters (as strings)
    timeout = data['timeout']
    retries = data['retries']
    seg_timeout = data['seg_timeout']
    
    # Data Source Enabled status (convert boolean back to JSON string "true"/"false")
    # Note: data['ds_enabled'] is True/False here, convert to string for substitution
    ds_enabled = str(data['ds_enabled']).lower()
    
    # Verbosity Level
    debug_mode = data.get('debug_mode', False)
    verbosity_level = 2 if debug_mode else 1
    
    udmi_version = data['udmi_version']
        
    def log(level, message):
        """Prints a message if its level is less than or equal to the configured verbosity level."""
        if level <= verbosity_level:
            print(message)
    
    # --- Template Selection Logic ---
    if udmi_version == '5.3.*':
        log(1, "--- Using UDMI V5.3.* Configuration Templates ---")
        T_BACNET_LD = TEMPLATE_BACNET_LOCALDEVICE_V5_3
        T_BACNET_DS = TEMPLATE_BACNET_DATASOURCE_V5_3
        T_MANGO_SYS = TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_3
        T_MANGO_PUB = TEMPLATE_MANGO_UDMI_PUBLISHER_V5_3
        udmi_config_xid = "N/A"
    elif udmi_version == '5.4.*':
        log(1, "--- Using UDMI V5.4.* Configuration Templates ---")
        T_BACNET_LD = TEMPLATE_BACNET_LOCALDEVICE_V5_4
        T_BACNET_DS = TEMPLATE_BACNET_DATASOURCE_V5_4
        T_MANGO_SYS = TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_4
        T_MANGO_PUB = TEMPLATE_MANGO_UDMI_PUBLISHER_V5_4
        udmi_config_xid = generate_udmi_config_xid(registry_id, gcp_project_id)
    else:
        log(0, f"ERROR: Unsupported UDMI version selected: {udmi_version}. Exiting.")
        return

    # Level 1: Info (Default)
    log(1, f"Running script with the following parameters:")
    log(1, pprint(data))
    log(1, f"UDMI Version selected: {udmi_version}")
    log(1, "\n" + "="*50 + "\n")

    # CLI mode validation is handled in main() but must be re-checked here
    if not (input_file and output_prefix and os.path.exists(input_file)):
         log(0, "Script finished without processing due to missing file or prefix.")
         return

    # --- EFFICIENCY BOOST: Read all sheets into memory once ---
    log(1, "Reading all data from spreadsheet...")
    all_sheets_data = read_all_sheets(input_file)
    
    if not all_sheets_data:
        log(0, "FATAL ERROR: Could not read data from input file. Exiting.")
        return

    devices_data = all_sheets_data.get("devices")
    
    if devices_data is None:
        log(0, "ERROR: 'devices' sheet not found or empty. Exiting.")
        return

    # Report device data (first few rows)
    log(1, f"Data from sheet 'devices':")
    if verbosity_level >= 1:
        print(tabulate(devices_data.head(), headers='keys', tablefmt='psql')) 
    
    output_mango_bacnet_config_filename = f"{output_prefix}_bacnet_config.json"
    output_mango_udmi_publisher_filename = f"{output_prefix}_udmi_publisher.json"

    # --- Pass 1: Count Points and Determine Proxy Devices (Preparation) ---
    proxy_device_set = set()
    points_to_be_exported = 0
    
    log(1, "\nCounting points and identifying proxy devices...")
    for index, device in devices_data.iterrows():
        sanitized_device_name = device.get('sanitized_device_name')
        if sanitized_device_name == "BAC0" or isNaN(sanitized_device_name):
            continue

        points_data = all_sheets_data.get(sanitized_device_name) 
        
        if points_data is not None:
            for index, point in points_data.iterrows():
                cloud_device_id = point.get('cloud_device_id')  
                cloud_point_name = point.get('cloud_point_name')
                
                if cloud_device_id and cloud_point_name and not isNaN(cloud_device_id) and not isNaN(cloud_point_name):
                    proxy_device_set.add(cloud_device_id)
                    points_to_be_exported += 1
    
    # Final preparation steps (Proxy devices, RSA keys)
    proxy_devices = sorted(list(proxy_device_set))
    log(1, f"Total points to export: {points_to_be_exported}")
    log(1, f"Proxy devices: {proxy_devices}")

    mango_publisher_xid = f"PUB_UDMI_BACNET_{publisher_name}"
    mango_proxydevices_array_string = "[%s]" % ','.join([f'{{"name": "{d}"}}' for d in proxy_devices])

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = key.private_bytes(encoding=crypto_serialization.Encoding.PEM, format=crypto_serialization.PrivateFormat.PKCS8, encryption_algorithm=crypto_serialization.NoEncryption())
    public_key_pem = key.public_key().public_bytes(encoding=crypto_serialization.Encoding.PEM, format=crypto_serialization.PublicFormat.SubjectPublicKeyInfo)
    
    mango_rsa_privatekey = private_key_pem.decode('utf-8').replace("\n", "\\n")
    mango_rsa_publickey = public_key_pem.decode('utf-8').replace("\n", "\\n")

    # --- Pass 2: Generate JSON Content (Optimized Single Write) ---
    log(1, "\nGenerating JSON files...")

    # Open both files for writing simultaneously
    with open(output_mango_bacnet_config_filename, 'w') as out_bacnet, \
         open(output_mango_udmi_publisher_filename, 'w') as out_udmi:

        # 1. BACNET CONFIGURATION FILE START
        out_bacnet.write("{\n")
        out_bacnet.write(T_BACNET_LD.substitute(
            broadcast_address=broadcast_address, 
            bacnet_localdevice_id=bacnet_localdevice_id,
            timeout=timeout,
            retries=retries,
            seg_timeout=seg_timeout
        ))
        out_bacnet.write(",\n")
        
        # 2. BACNET DATA SOURCES
        out_bacnet.write('"dataSources": [\n')
        
        unique_device_sheets = sorted([d for d in all_sheets_data.keys() if d != 'devices'])
        unique_device_names = [d for d in unique_device_sheets if d != "BAC0"]
        
        if unique_ds:
            for i, device_name in enumerate(unique_device_names):
                data_source_xid = f"DS_BACNET_{device_name}"
                data_source_name = f"BACNET_{device_name}"
                out_bacnet.write(T_BACNET_DS.substitute(
                    data_source_xid=data_source_xid, 
                    data_source_name=data_source_name,
                    data_source_enabled=ds_enabled 
                ))
                if i < len(unique_device_names) - 1:
                    out_bacnet.write(",\n")
        else:
            data_source_xid = "DS_BACNET"
            data_source_name = "BACNET"
            out_bacnet.write(T_BACNET_DS.substitute(
                data_source_xid=data_source_xid, 
                data_source_name=data_source_name,
                data_source_enabled=ds_enabled 
            ))
            
        out_bacnet.write('\n],\n"dataPoints": [\n')
        
        # 3. UDMI PUBLISHER FILE START
        out_udmi.write('{\n')
        
        # System Settings Template (V5.3 vs V5.4)
        if udmi_version == '5.4.*':
            out_udmi.write(T_MANGO_SYS.substitute(
                gcp_project_id=data['project'], 
                gcp_cloud_region=data['region'], 
                registry_id=data['registry'], 
                site_id=data['site'],
                udmi_config_xid=udmi_config_xid,
                hostname=data['hostname'] 
            ))
        else:
            out_udmi.write(T_MANGO_SYS.substitute(
                gcp_project_id=data['project'], 
                gcp_cloud_region=data['region'], 
                registry_id=data['registry'], 
                site_id=data['site']
            ))


        out_udmi.write('\n  "publishers": [\n')
        
        # Publisher Template (V5.3 vs V5.4)
        if udmi_version == '5.4.*':
            out_udmi.write(T_MANGO_PUB.substitute(
                mango_publisher_xid=mango_publisher_xid,
                mango_publisher_name=publisher_name,
                publisher_device_id=publisher_name,
                mango_proxydevices_array=mango_proxydevices_array_string,
                mango_rsa_privatekey=mango_rsa_privatekey,
                mango_rsa_publickey=mango_rsa_publickey,
                udmi_config_xid=udmi_config_xid
            ))
        else:
            out_udmi.write(T_MANGO_PUB.substitute(
                mango_publisher_xid=mango_publisher_xid,
                mango_publisher_name=publisher_name,
                mango_proxydevices_array=mango_proxydevices_array_string,
                mango_rsa_privatekey=mango_rsa_privatekey,
                mango_rsa_publickey=mango_rsa_publickey
            ))


        out_udmi.write('],\n"publishedPoints": [')
        
        # 4. DATA POINT GENERATION LOOP
        current_point_count = 0
        
        OBJECT_TYPE = {
            'analogInput': 'ANALOG_INPUT', 'analogOutput': 'ANALOG_OUTPUT', 
            'analogValue': 'ANALOG_VALUE', 'binaryInput': 'BINARY_INPUT', 
            'binaryOutput': 'BINARY_OUTPUT', 'binaryValue': 'BINARY_VALUE', 
            'multiStateInput': 'MULTISTATE_INPUT', 'multiStateOutput': 'MULTISTATE_OUTPUT', 
            'multiStateValue': 'MULTISTATE_VALUE'
        }
        
        for index, device in devices_data.iterrows():
            sanitized_device_name = device.get('sanitized_device_name')
            device_id = device.get('device_id')

            if sanitized_device_name == "BAC0" or isNaN(sanitized_device_name):
                continue
            
            points_data = all_sheets_data.get(sanitized_device_name)
            if points_data is None:
                continue

            current_data_source_xid = f"DS_BACNET_{sanitized_device_name}" if unique_ds else "DS_BACNET"
            
            log(2, f"\nProcessing device: {sanitized_device_name} (ID: {device_id})")
            
            for index, point in points_data.iterrows():
                cloud_device_id = point.get('cloud_device_id')
                cloud_point_name = point.get('cloud_point_name')
                
                if cloud_device_id and cloud_point_name and not isNaN(cloud_device_id) and not isNaN(cloud_point_name):
                    
                    current_point_count += 1
                    
                    # Prepare point data
                    mango_point_name = str(cloud_point_name)
                    mango_device_id = str(cloud_device_id)
                    
                    point_obj_str = str(point.get('object', '')).split(":")
                    point_remote_object_instance_number = point_obj_str[1] if len(point_obj_str) > 1 else '0'
                    point_remote_device_instance_number = device_id
                    point_object_type = OBJECT_TYPE.get(point_obj_str[0], 'ANALOG_VALUE')

                    bacnet_point_property_name = point_object_type
                    bacnet_point_description = point.get('description', '')
                    bacnet_point_description = "" if isNaN(bacnet_point_description) else str(bacnet_point_description)
                    
                    mango_point_xid = f"DP_{device_id}_{point_object_type}_{point_remote_object_instance_number}"
                    bacnet_point_name = point.get('point_name', '')
                    bacnet_device_name = point.get('device_name', '')

                    # Generate BACNET DATAPOINT
                    bacnet_dp_json = TEMPLATE_BACNET_DATAPOINT.substitute(
                        mango_point_name=mango_point_name, point_data_type="NUMERIC",
                        mango_device_name=mango_device_id, bacnet_point_name=bacnet_point_name,
                        point_object_type=point_object_type,
                        point_remote_object_instance_number=point_remote_object_instance_number,
                        point_remote_device_instance_number=point_remote_device_instance_number,
                        bacnet_device_name=bacnet_device_name,
                        bacnet_point_property_name=bacnet_point_property_name,
                        bacnet_point_description=bacnet_point_description,
                        mango_point_xid=mango_point_xid,
                        data_source_xid=current_data_source_xid
                    )
                    out_bacnet.write(bacnet_dp_json)

                    # Generate UDMI PUBLISHED POINT
                    udmi_pp_json = TEMPLATE_MANGO_PUBLISHED_POINT.substitute(
                        point_name=mango_point_name, point_xid=mango_point_xid,
                        device_name=mango_device_id, mango_publisher_xid=mango_publisher_xid
                    )
                    out_udmi.write(udmi_pp_json)

                    log(2, f"  -> Point: {mango_point_name} (XID: {mango_point_xid})")

                    # Add comma if this is NOT the last point
                    if current_point_count < points_to_be_exported:
                        out_bacnet.write(',\n')
                        out_udmi.write(',\n')
            
        # 5. CLOSING FILES
        out_bacnet.write("]\n}")
        out_udmi.write("]\n}")
        
    log(0, "\n" + "="*50)
    log(0, f"Successfully created files:")
    log(0, f"- {output_mango_bacnet_config_filename}")
    log(0, f"- {output_mango_udmi_publisher_filename}")
    log(0, "="*50)

# --- MAIN EXECUTION BLOCK ---

def main():
    # Define default values for CLI mode (matching GUI defaults for consistency)
    CLI_DEFAULTS = {
        'localdevice': '98777',
        'broadcast': '255.255.255.255',
        'publisher': 'CGWV-1',
        'project': 'bos-platform-prod',
        'region': 'us-central1',
        'registry': 'ZZ-ABC-DEF',
        'site': 'ZZ-ABC-DEF',
        'hostname': 'mqtt.bos.goog', 
        'unique': True,
        'timeout': 30000,
        'retries': 0,
        'segtimeout': 10000,
        'ds_enabled': 'True', # Use string 'True'/'False' for CLI argument compatibility
        'udmi_version': '5.4.*',
        'verbose': False # Maps to debug_mode
    }

    # --- Custom Help Text ---
    epilog_text = f"""
    --- EXAMPLES ---
    1. Interactive GUI Mode (no arguments):
       $ ./sheet2mangojson.py

    2. CLI Quick Run (using defaults for all optional fields):
       $ ./sheet2mangojson.py -i bacnet-scan.xlsx -o ZZ-ABC-DEF_round-1

    3. CLI with Custom BACnet settings and V5.3 UDMI:
       $ ./sheet2mangojson.py -i scan.xlsx -o config -l 12345 --udmi-version 5.3.* --timeout 15000 --retries 1 --ds-enabled False
    """
    
    parser = argparse.ArgumentParser(
        description="Converts BACnet spreadsheet data to Mango JSON configuration files.",
        formatter_class=argparse.RawTextHelpFormatter, # Allows custom formatting for epilog
        epilog=epilog_text
    )
    
    # --- Mode Detection (Primary Arguments - Clearly Marked as Required) ---
    parser.add_argument(
        "-i", "--input", default=None,
        required=False, # Set to False here, handled by custom logic below
        help="[REQUIRED] Input spreadsheet file path."
    )
    parser.add_argument(
        "-o", "--output", default=None,
        required=False, # Set to False here, handled by custom logic below
        help="[REQUIRED] Output file prefix."
    )

    # --- General/Verbosity Arguments ---
    parser.add_argument(
        "-v", "--verbose", action="store_true", default=CLI_DEFAULTS['verbose'],
        help=f"[OPTIONAL] Enable detailed debug output (Default: {CLI_DEFAULTS['verbose']})."
    )
    parser.add_argument(
        "--udmi-version", type=str, choices=['5.4.*', '5.3.*'], default=CLI_DEFAULTS['udmi_version'],
        help=f"[OPTIONAL] Select UDMI driver version (Default: {CLI_DEFAULTS['udmi_version']})."
    )

    # --- BACnet Configuration Arguments ---
    parser.add_argument(
        "-u", "--unique", action="store_true", default=CLI_DEFAULTS['unique'],
        help=f"[OPTIONAL] Create unique BACnet data sources per device (Default: {CLI_DEFAULTS['unique']})."
    )
    parser.add_argument(
        "--ds-enabled", type=str, choices=['True', 'False'], default=CLI_DEFAULTS['ds_enabled'],
        help=f"[OPTIONAL] Set BACnet Data Source 'enabled' status (Default: {CLI_DEFAULTS['ds_enabled']})."
    )
    parser.add_argument(
        "-l", "--localdevice", type=int, default=int(CLI_DEFAULTS['localdevice']),
        help=f"[OPTIONAL] ID of the BACnet Local Device in Mango (Default: {CLI_DEFAULTS['localdevice']})."
    )
    parser.add_argument(
        "-b", "--broadcast", type=str, default=CLI_DEFAULTS['broadcast'],
        help=f"[OPTIONAL] Broadcast address for the BACnet Local Device (Default: {CLI_DEFAULTS['broadcast']})."
    )
    parser.add_argument(
        "--timeout", type=int, default=CLI_DEFAULTS['timeout'],
        help=f"[OPTIONAL] BACnet Local Device 'timeout' in ms (Default: {CLI_DEFAULTS['timeout']} ms)."
    )
    parser.add_argument(
        "--retries", type=int, default=CLI_DEFAULTS['retries'],
        help=f"[OPTIONAL] BACnet Local Device 'retries' count (Default: {CLI_DEFAULTS['retries']})."
    )
    parser.add_argument(
        "--segtimeout", type=int, default=CLI_DEFAULTS['segtimeout'],
        help=f"[OPTIONAL] BACnet Local Device 'segTimeout' in ms (Default: {CLI_DEFAULTS['segtimeout']} ms)."
    )

    # --- UDMI Publisher Arguments ---
    parser.add_argument(
        "-p", "--publisher", type=str, default=CLI_DEFAULTS['publisher'],
        help=f"[OPTIONAL] Name for the Mango UDMI publisher device (Default: {CLI_DEFAULTS['publisher']})."
    )
    parser.add_argument(
        "-j", "--project", type=str, default=CLI_DEFAULTS['project'],
        help=f"[OPTIONAL] GCP project ID for the UDMI publisher (Default: {CLI_DEFAULTS['project']})."
    )
    parser.add_argument(
        "-g", "--region", type=str, default=CLI_DEFAULTS['region'],
        help=f"[OPTIONAL] GCP region for the UDMI publisher (Default: {CLI_DEFAULTS['region']})."
    )
    parser.add_argument(
        "-r", "--registry", type=str, default=CLI_DEFAULTS['registry'],
        help=f"[OPTIONAL] IoT Core registry ID for the UDMI publisher (Default: {CLI_DEFAULTS['registry']})."
    )
    parser.add_argument(
        "-s", "--site", type=str, default=CLI_DEFAULTS['site'],
        help=f"[OPTIONAL] IoT Core site name for the UDMI publisher (Default: {CLI_DEFAULTS['site']})."
    )
    parser.add_argument(
        "--hostname", type=str, default=CLI_DEFAULTS['hostname'],
        help=f"[OPTIONAL] UDMI Hostname (used for V5.4.* templates) (Default: {CLI_DEFAULTS['hostname']})."
    )

    # --- Execution Logic ---
    
    # Check if any arguments were passed (excluding the script name itself)
    if len(sys.argv) > 1:
        # CLI Mode: Parse arguments
        args = parser.parse_args()
        
        # Simple validation for required CLI fields
        if not args.input or not args.output:
            # Manually trigger help and error if required fields are missing
            print("ERROR: Arguments -i/--input and -o/--output are required when running in CLI mode.\n")
            parser.print_help()
            sys.exit(1)

        show_title()
        # Map CLI arguments to the standardized data dictionary structure
        data = map_args_to_data(args, CLI_DEFAULTS)
        
        # Run core logic
        run_core_logic(data)
    else:
        # GUI Mode: Launch GUI
        root = tk.Tk()
        app = ConfigGUI(root)
        root.mainloop()

if __name__ == "__main__":
    main()

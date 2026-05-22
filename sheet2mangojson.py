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
__version__ = "0.41"
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
import sys
import json
from string import Template
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend
import tkinter as tk
from tkinter import filedialog, messagebox


# ----------------------------------------------------------------------
# --- TEMPLATE DEFINITIONS (V5.5.*) ---
# ----------------------------------------------------------------------

TEMPLATE_MANGO_UDMI_PUBLISHER_V5_5 = Template("""
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
    "udmiDeviceIdentifiers": ${udmi_device_identifiers},
    "cacheDiscardSize": 50000,
    "cacheWarningSize": 10000,
    "publishAttributeChanges": false,
    "publishPointEvents": false,
    "sendSnapshot": false,
    "snapshotSendPeriods": 5
}""")

TEMPLATE_MANGO_PUBLISHED_POINT_V5_5 = Template('''
    {
      "xid": "PP_${point_xid}",
      "name": "${point_name}",
      "enabled": true,
      "dataPointXid": "${point_xid}",
      "deviceName": "${device_name}",
      "publisherXid": "${mango_publisher_xid}"
    }''')


# ----------------------------------------------------------------------
# --- TEMPLATE DEFINITIONS (V5.4.*) ---
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
# --- TEMPLATE DEFINITIONS (V5.3.*) ---
# ----------------------------------------------------------------------

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

TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_3 = Template("""
  "systemSettings": {
    "udmi.config": "{\\"iotProvider\\": \\"GBOS\\", \\"projectId\\": \\"${gcp_project_id}\\", \\"cloudRegion\\": \\"${gcp_cloud_region}\\", \\"registryId\\": \\"${registry_id}\\", \\"site\\": \\"${site_id}\\"}"
  },
""")

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

# Common Templates
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
    try:
        return pd.read_excel(file_path, sheet_name=None)
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during file read: {e}")
        return None

def isNaN(num):
    try:
        import numpy as np
        return pd.isna(num) or np.isnan(num)
    except Exception:
        return num != num

def generate_udmi_config_xid(registry_id, project_id):
    xid = f"UDMI-CONFIG-{project_id}-{registry_id}"
    return xid.upper().replace('_', '-').replace('.', '-')


# --- GUI CLASS ---
class ConfigGUI:
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
            'udmi_version': tk.StringVar(value='5.5.*'), 
        }
        
        self.config_sections = [
            ("1. File & General Configuration", [
                ("Input Spreadsheet", 'input_file', True),
                ("Output File Prefix", 'output_prefix', True),
                ("UDMI Driver Version", 'udmi_version', ['5.5.*', '5.4.*', '5.3.*'], 'dropdown'),
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

        main_frame = tk.Frame(master)
        main_frame.pack(padx=10, pady=10, fill='both', expand=True)

        frame_row = 0
        for section_title, fields_list in self.config_sections:
            label_frame = tk.LabelFrame(main_frame, text=section_title, padx=10, pady=10)
            label_frame.grid(row=frame_row, column=0, sticky="ew", pady=10)
            
            r = 0
            for item in fields_list:
                if len(item) == 4 and item[3] == 'dropdown':
                    label_text, var_name, options, _ = item
                    var = self.defaults.get(var_name)
                    tk.Label(label_frame, text=f"{label_text}:").grid(row=r, column=0, sticky='w', padx=5, pady=2)
                    option_menu = tk.OptionMenu(label_frame, var, *options)
                    option_menu.grid(row=r, column=1, padx=5, pady=2, sticky='ew')
                    self.fields[var_name] = var
                else: 
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
        if not input_file or not os.path.exists(input_file) or not output_prefix:
            messagebox.showerror("Validation Error", "Input file and prefix are required.")
            return
        data = {k: v.get() if isinstance(v, (tk.StringVar, tk.BooleanVar)) else v for k, v in self.fields.items()}
        self.master.destroy()
        run_core_logic(data)

def map_args_to_data(args, defaults):
    data = {
        'input_file': args.input, 'output_prefix': args.output,
        'localdevice': str(args.localdevice), 'broadcast': args.broadcast,
        'publisher': args.publisher, 'project': args.project,
        'region': args.region, 'registry': args.registry,
        'site': args.site, 'hostname': args.hostname,
        'unique': args.unique, 'timeout': str(args.timeout),
        'retries': str(args.retries), 'seg_timeout': str(args.segtimeout),
        'ds_enabled': args.ds_enabled.lower() == 'true',
        'debug_mode': args.verbose, 'udmi_version': args.udmi_version,
    }
    return data

# --- CORE LOGIC FUNCTION ---

def run_core_logic(data):
    show_title()

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
    timeout = data['timeout']
    retries = data['retries']
    seg_timeout = data['seg_timeout']
    ds_enabled = str(data['ds_enabled']).lower()
    
    debug_mode = data.get('debug_mode', False)
    verbosity_level = 2 if debug_mode else 1
    udmi_version = data['udmi_version']
        
    def log(level, message):
        if level <= verbosity_level:
            print(message)
    
    # --- Template Selection ---
    if udmi_version == '5.3.*':
        log(1, "--- Using UDMI V5.3.* Configuration Templates ---")
        T_BACNET_LD = TEMPLATE_BACNET_LOCALDEVICE_V5_3
        T_BACNET_DS = TEMPLATE_BACNET_DATASOURCE_V5_3
        T_MANGO_SYS = TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_3
        T_MANGO_PUB = TEMPLATE_MANGO_UDMI_PUBLISHER_V5_3
        T_MANGO_PP  = TEMPLATE_MANGO_PUBLISHED_POINT
        udmi_config_xid = "N/A"
    elif udmi_version == '5.4.*':
        log(1, "--- Using UDMI V5.4.* Configuration Templates ---")
        T_BACNET_LD = TEMPLATE_BACNET_LOCALDEVICE_V5_4
        T_BACNET_DS = TEMPLATE_BACNET_DATASOURCE_V5_4
        T_MANGO_SYS = TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_4
        T_MANGO_PUB = TEMPLATE_MANGO_UDMI_PUBLISHER_V5_4
        T_MANGO_PP  = TEMPLATE_MANGO_PUBLISHED_POINT
        udmi_config_xid = generate_udmi_config_xid(registry_id, gcp_project_id)
    elif udmi_version == '5.5.*':
        log(1, "--- Using UDMI V5.5.* Configuration Templates ---")
        T_BACNET_LD = TEMPLATE_BACNET_LOCALDEVICE_V5_4
        T_BACNET_DS = TEMPLATE_BACNET_DATASOURCE_V5_4
        T_MANGO_SYS = TEMPLATE_MANGO_SYSTEM_SETTINGS_V5_4
        T_MANGO_PUB = TEMPLATE_MANGO_UDMI_PUBLISHER_V5_5
        T_MANGO_PP  = TEMPLATE_MANGO_PUBLISHED_POINT_V5_5
        udmi_config_xid = generate_udmi_config_xid(registry_id, gcp_project_id)
    else:
        log(0, f"ERROR: Unsupported UDMI version selected: {udmi_version}. Exiting.")
        return

    log(1, f"Running script with selected UDMI Version: {udmi_version}")

    if not (input_file and output_prefix and os.path.exists(input_file)):
         log(0, "Script finished without processing due to missing file or prefix.")
         return

    all_sheets_data = read_all_sheets(input_file)
    if not all_sheets_data:
        log(0, "FATAL ERROR: Could not read data from input file. Exiting.")
        return

    devices_data = all_sheets_data.get("devices")
    if devices_data is None:
        log(0, "ERROR: 'devices' sheet not found or empty. Exiting.")
        return

    output_mango_bacnet_config_filename = f"{output_prefix}_bacnet_config.json"
    output_mango_udmi_publisher_filename = f"{output_prefix}_udmi_publisher.json"

    # --- Pass 1: Count Points and Determine Proxy Devices ---
    proxy_device_set = set()
    points_to_be_exported = 0
    
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
    
    proxy_devices = sorted(list(proxy_device_set))
    mango_publisher_xid = f"PUB_UDMI_BACNET_{publisher_name}"
    mango_proxydevices_array_string = "[%s]" % ','.join([f'{{"name": "{d}"}}' for d in proxy_devices])

    # Generate RSA Keys
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = key.private_bytes(encoding=crypto_serialization.Encoding.PEM, format=crypto_serialization.PrivateFormat.PKCS8, encryption_algorithm=crypto_serialization.NoEncryption())
    public_key_pem = key.public_key().public_bytes(encoding=crypto_serialization.Encoding.PEM, format=crypto_serialization.PublicFormat.SubjectPublicKeyInfo)
    mango_rsa_privatekey = private_key_pem.decode('utf-8').replace("\n", "\\n")
    mango_rsa_publickey = public_key_pem.decode('utf-8').replace("\n", "\\n")

    # --- UDMI 5.5.* Device Identifiers Array Build ---
    udmi_device_identifiers_string = "{}"
    if udmi_version == '5.5.*':
        direct_props = [
            {"dataPointXid": "NONE", "key": "HW:make", "required": False, "value": "NONE"},
            {"dataPointXid": "NONE", "key": "HW:model", "required": False, "value": "NONE"},
            {"dataPointXid": "NONE", "key": "HW:sku", "required": False, "value": "NONE"},
            {"dataPointXid": "NONE", "key": "HW:rev", "required": False, "value": "NONE"},
            {"dataPointXid": "NONE", "key": "SW:firmware", "required": False, "value": "NONE"},
            {"dataPointXid": "NONE", "key": "SW:os", "required": False, "value": "NONE"},
            {"dataPointXid": "NONE", "key": "SW:driver", "required": False, "value": "NONE"}
        ]
        proxied_props = [{"dataPointXid": "NONE", "key": "SYS:serial_no", "required": False, "value": "NONE"}] + direct_props
        
        identifiers = {
            "DIRECT": { publisher_name: direct_props },
            "PROXIED": { "": proxied_props }
        }
        for dev in proxy_devices:
            identifiers["PROXIED"][dev] = proxied_props
            
        udmi_device_identifiers_string = json.dumps(identifiers, indent=4).replace('\n', '\n    ')


    # --- Pass 2: Generate JSON Content ---
    with open(output_mango_bacnet_config_filename, 'w') as out_bacnet, \
         open(output_mango_udmi_publisher_filename, 'w') as out_udmi:

        # 1. BACNET FILE
        out_bacnet.write("{\n")
        out_bacnet.write(T_BACNET_LD.substitute(
            broadcast_address=broadcast_address, bacnet_localdevice_id=bacnet_localdevice_id,
            timeout=timeout, retries=retries, seg_timeout=seg_timeout
        ))
        out_bacnet.write(",\n")
        out_bacnet.write('"dataSources": [\n')
        
        unique_device_sheets = sorted([d for d in all_sheets_data.keys() if d != 'devices'])
        unique_device_names = [d for d in unique_device_sheets if d != "BAC0"]
        
        if unique_ds:
            for i, device_name in enumerate(unique_device_names):
                out_bacnet.write(T_BACNET_DS.substitute(
                    data_source_xid=f"DS_BACNET_{device_name}", data_source_name=f"BACNET_{device_name}",
                    data_source_enabled=ds_enabled 
                ))
                if i < len(unique_device_names) - 1: out_bacnet.write(",\n")
        else:
            out_bacnet.write(T_BACNET_DS.substitute(
                data_source_xid="DS_BACNET", data_source_name="BACNET", data_source_enabled=ds_enabled 
            ))
            
        out_bacnet.write('\n],\n"dataPoints": [\n')
        
        # 2. UDMI FILE
        out_udmi.write('{\n')
        if udmi_version in ['5.4.*', '5.5.*']:
            out_udmi.write(T_MANGO_SYS.substitute(
                gcp_project_id=data['project'], gcp_cloud_region=data['region'], 
                registry_id=data['registry'], site_id=data['site'], udmi_config_xid=udmi_config_xid, hostname=data['hostname'] 
            ))
        else:
            out_udmi.write(T_MANGO_SYS.substitute(
                gcp_project_id=data['project'], gcp_cloud_region=data['region'], registry_id=data['registry'], site_id=data['site']
            ))

        out_udmi.write('\n  "publishers": [\n')
        if udmi_version == '5.5.*':
            out_udmi.write(T_MANGO_PUB.substitute(
                mango_publisher_xid=mango_publisher_xid, mango_publisher_name=publisher_name,
                publisher_device_id=publisher_name, mango_proxydevices_array=mango_proxydevices_array_string,
                mango_rsa_privatekey=mango_rsa_privatekey, mango_rsa_publickey=mango_rsa_publickey,
                udmi_config_xid=udmi_config_xid, udmi_device_identifiers=udmi_device_identifiers_string
            ))
        elif udmi_version == '5.4.*':
            out_udmi.write(T_MANGO_PUB.substitute(
                mango_publisher_xid=mango_publisher_xid, mango_publisher_name=publisher_name,
                publisher_device_id=publisher_name, mango_proxydevices_array=mango_proxydevices_array_string,
                mango_rsa_privatekey=mango_rsa_privatekey, mango_rsa_publickey=mango_rsa_publickey, udmi_config_xid=udmi_config_xid
            ))
        else:
            out_udmi.write(T_MANGO_PUB.substitute(
                mango_publisher_xid=mango_publisher_xid, mango_publisher_name=publisher_name,
                mango_proxydevices_array=mango_proxydevices_array_string, mango_rsa_privatekey=mango_rsa_privatekey, mango_rsa_publickey=mango_rsa_publickey
            ))

        out_udmi.write('],\n"publishedPoints": [')
        
        # 3. POINTS LOOP
        current_point_count = 0
        OBJECT_TYPE = {
            'analogInput': 'ANALOG_INPUT', 'analogOutput': 'ANALOG_OUTPUT', 'analogValue': 'ANALOG_VALUE', 
            'binaryInput': 'BINARY_INPUT', 'binaryOutput': 'BINARY_OUTPUT', 'binaryValue': 'BINARY_VALUE', 
            'multiStateInput': 'MULTISTATE_INPUT', 'multiStateOutput': 'MULTISTATE_OUTPUT', 'multiStateValue': 'MULTISTATE_VALUE'
        }
        
        for index, device in devices_data.iterrows():
            sanitized_device_name = device.get('sanitized_device_name')
            device_id = device.get('device_id')
            if sanitized_device_name == "BAC0" or isNaN(sanitized_device_name): continue
            
            points_data = all_sheets_data.get(sanitized_device_name)
            if points_data is None: continue

            current_data_source_xid = f"DS_BACNET_{sanitized_device_name}" if unique_ds else "DS_BACNET"
            for index, point in points_data.iterrows():
                cloud_device_id = point.get('cloud_device_id')
                cloud_point_name = point.get('cloud_point_name')
                
                if cloud_device_id and cloud_point_name and not isNaN(cloud_device_id) and not isNaN(cloud_point_name):
                    current_point_count += 1
                    
                    mango_point_name = str(cloud_point_name)
                    mango_device_id = str(cloud_device_id)
                    point_obj_str = str(point.get('object', '')).split(":")
                    point_remote_object_instance_number = point_obj_str[1] if len(point_obj_str) > 1 else '0'
                    point_object_type = OBJECT_TYPE.get(point_obj_str[0], 'ANALOG_VALUE')
                    
                    bacnet_point_property_name = point_object_type
                    bacnet_point_description = "" if isNaN(point.get('description', '')) else str(point.get('description', ''))
                    mango_point_xid = f"DP_{device_id}_{point_object_type}_{point_remote_object_instance_number}"
                    
                    out_bacnet.write(TEMPLATE_BACNET_DATAPOINT.substitute(
                        mango_point_name=mango_point_name, point_data_type="NUMERIC",
                        mango_device_name=mango_device_id, bacnet_point_name=point.get('point_name', ''),
                        point_object_type=point_object_type, point_remote_object_instance_number=point_remote_object_instance_number,
                        point_remote_device_instance_number=device_id, bacnet_device_name=point.get('device_name', ''),
                        bacnet_point_property_name=bacnet_point_property_name, bacnet_point_description=bacnet_point_description,
                        mango_point_xid=mango_point_xid, data_source_xid=current_data_source_xid
                    ))

                    out_udmi.write(T_MANGO_PP.substitute(
                        point_name=mango_point_name, point_xid=mango_point_xid,
                        device_name=mango_device_id, mango_publisher_xid=mango_publisher_xid
                    ))

                    if current_point_count < points_to_be_exported:
                        out_bacnet.write(',\n')
                        out_udmi.write(',\n')
            
        out_bacnet.write("]\n}")
        out_udmi.write("]\n}")

# --- MAIN EXECUTION BLOCK ---

def main():
    CLI_DEFAULTS = {
        'localdevice': '98777', 'broadcast': '255.255.255.255',
        'publisher': 'CGWV-1', 'project': 'bos-platform-prod',
        'region': 'us-central1', 'registry': 'ZZ-ABC-DEF',
        'site': 'ZZ-ABC-DEF', 'hostname': 'mqtt.bos.goog', 
        'unique': True, 'timeout': 30000, 'retries': 0,
        'segtimeout': 10000, 'ds_enabled': 'True', 
        'udmi_version': '5.5.*', 'verbose': False 
    }

    parser = argparse.ArgumentParser(description="Converts BACnet spreadsheet data to Mango JSON configuration files.")
    parser.add_argument("-i", "--input", default=None, required=False, help="[REQUIRED] Input spreadsheet file path.")
    parser.add_argument("-o", "--output", default=None, required=False, help="[REQUIRED] Output file prefix.")
    parser.add_argument("-v", "--verbose", action="store_true", default=CLI_DEFAULTS['verbose'])
    parser.add_argument("--udmi-version", type=str, choices=['5.5.*', '5.4.*', '5.3.*'], default=CLI_DEFAULTS['udmi_version'])
    parser.add_argument("-u", "--unique", action="store_true", default=CLI_DEFAULTS['unique'])
    parser.add_argument("--ds-enabled", type=str, choices=['True', 'False'], default=CLI_DEFAULTS['ds_enabled'])
    parser.add_argument("-l", "--localdevice", type=int, default=int(CLI_DEFAULTS['localdevice']))
    parser.add_argument("-b", "--broadcast", type=str, default=CLI_DEFAULTS['broadcast'])
    parser.add_argument("--timeout", type=int, default=CLI_DEFAULTS['timeout'])
    parser.add_argument("--retries", type=int, default=CLI_DEFAULTS['retries'])
    parser.add_argument("--segtimeout", type=int, default=CLI_DEFAULTS['segtimeout'])
    parser.add_argument("-p", "--publisher", type=str, default=CLI_DEFAULTS['publisher'])
    parser.add_argument("-j", "--project", type=str, default=CLI_DEFAULTS['project'])
    parser.add_argument("-g", "--region", type=str, default=CLI_DEFAULTS['region'])
    parser.add_argument("-r", "--registry", type=str, default=CLI_DEFAULTS['registry'])
    parser.add_argument("-s", "--site", type=str, default=CLI_DEFAULTS['site'])
    parser.add_argument("--hostname", type=str, default=CLI_DEFAULTS['hostname'])

    if len(sys.argv) > 1:
        args = parser.parse_args()
        if not args.input or not args.output:
            print("ERROR: Arguments -i/--input and -o/--output are required when running in CLI mode.\n")
            sys.exit(1)
        data = map_args_to_data(args, CLI_DEFAULTS)
        run_core_logic(data)
    else:
        root = tk.Tk()
        app = ConfigGUI(root)
        root.mainloop()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3

"""
  bacnet-write.py

  Writes a value to a specific BACnet point/object on a specific BACnet device using BAC0.

  [Usage]
  python3 bacnet-write.py -i <DEVICE_IP> -o <OBJECT_TYPE:INSTANCE> -w <VALUE> [options]
  python3 bacnet-write.py -b <DEVICE_ID> -p <POINT_NAME> -w <VALUE> [options]
"""

__author__ = "Francesco Anselmo"
__copyright__ = "Copyright 2025"
__credits__ = ["Francesco Anselmo"]
__license__ = "MIT"
__version__ = "0.45"
__maintainer__ = "Francesco Anselmo"
__email__ = "francesco.anselmo@gmail.com"
__status__ = "Dev"

import argparse
import os
import sys
import time
import BAC0
from tabulate import tabulate


OBJECT_TYPE_ALIASES = {
    "ai": "analogInput",
    "analoginput": "analogInput",
    "analog_input": "analogInput",
    "analog-input": "analogInput",
    "ao": "analogOutput",
    "analogoutput": "analogOutput",
    "analog_output": "analogOutput",
    "analog-output": "analogOutput",
    "av": "analogValue",
    "analogvalue": "analogValue",
    "analog_value": "analogValue",
    "analog-value": "analogValue",
    "bi": "binaryInput",
    "binaryinput": "binaryInput",
    "binary_input": "binaryInput",
    "binary-input": "binaryInput",
    "bo": "binaryOutput",
    "binaryoutput": "binaryOutput",
    "binary_output": "binaryOutput",
    "binary-output": "binaryOutput",
    "bv": "binaryValue",
    "binaryvalue": "binaryValue",
    "binary_value": "binaryValue",
    "binary-value": "binaryValue",
    "msi": "multiStateInput",
    "multistateinput": "multiStateInput",
    "multi_state_input": "multiStateInput",
    "multi-state-input": "multiStateInput",
    "mso": "multiStateOutput",
    "multistateoutput": "multiStateOutput",
    "multi_state_output": "multiStateOutput",
    "multi-state-output": "multiStateOutput",
    "msv": "multiStateValue",
    "multistatevalue": "multiStateValue",
    "multi_state_value": "multiStateValue",
    "multi-state-value": "multiStateValue",
    "csv": "characterstringValue",
    "characterstringvalue": "characterstringValue",
    "characterstring_value": "characterstringValue",
    "characterstring-value": "characterstringValue",
}

BINARY_OBJECT_TYPES = {"binaryInput", "binaryOutput", "binaryValue"}
MULTISTATE_OBJECT_TYPES = {"multiStateInput", "multiStateOutput", "multiStateValue"}
ANALOG_OBJECT_TYPES = {"analogInput", "analogOutput", "analogValue"}


def show_title():
    """Show the program title and version info."""
    title = r"""
      ____    _    ____            _                      _ _       
     | __ )  / \  / ___|_ __   ___| |_     __      ___ __(_) |_ ___ 
     |  _ \ / _ \| |   | '_ \ / _ \ __|____\ \ /\ / / '__| | __/ _ \
     | |_) / ___ \ |___| | | |  __/ ||______\ V  V /| |  | | ||  __/
     |____/_/   \_\____|_| |_|\___|\__|      \_/\_/ |_|  |_|\__\___|
    """

    print(title)
    print("-" * 60)
    print(f"Author:     {__author__}")
    print(f"Copyright:  {__copyright__}")
    print(f"License:    {__license__}")
    print(f"Version:    {__version__}")
    print(f"Status:     {__status__}")
    print("-" * 60)


def string_to_integer_list(comma_separated_string):
    if not comma_separated_string.strip():
        return []

    string_numbers = comma_separated_string.split(",")
    integer_list = []
    for s in string_numbers:
        try:
            number = int(s.strip())
            integer_list.append(number)
        except ValueError:
            print(f"Warning: Invalid integer format: '{s.strip()}', skipping.")
    return integer_list


def normalize_object_type(raw_type):
    """Normalizes a BACnet object type string or shorthand alias to BAC0 camelCase."""
    cleaned = str(raw_type).strip()
    lookup = cleaned.lower()
    if lookup in OBJECT_TYPE_ALIASES:
        return OBJECT_TYPE_ALIASES[lookup]
    return cleaned


def parse_object_spec(object_arg, instance_arg=None):
    """Parses object specification such as 'analogValue:1', 'analogValue 1', or ('analogValue', '1')."""
    if not object_arg:
        raise ValueError("Object specification is empty.")

    cleaned = str(object_arg).strip()
    if ":" in cleaned:
        parts = cleaned.split(":", 1)
        obj_type = normalize_object_type(parts[0])
        obj_instance = int(parts[1].strip())
        return obj_type, obj_instance

    if " " in cleaned and instance_arg in (None, ""):
        parts = cleaned.split(None, 1)
        obj_type = normalize_object_type(parts[0])
        obj_instance = int(parts[1].strip())
        return obj_type, obj_instance

    if instance_arg not in (None, ""):
        obj_type = normalize_object_type(cleaned)
        obj_instance = int(str(instance_arg).strip())
        return obj_type, obj_instance

    raise ValueError(
        f"Invalid object specification '{object_arg}'. "
        "Expected '<objectType>:<instance>' (e.g. 'analogValue:1') or use --instance."
    )


def format_write_value(raw_value, object_type, property_name="presentValue"):
    """Formats the value string for BAC0.write() according to the object type and property."""
    val_str = str(raw_value).strip()
    val_lower = val_str.lower()

    if val_lower in ("null", "none", "release", "relinquish"):
        return "null"

    if property_name == "outOfService":
        if val_lower in ("1", "true", "yes", "on", "active"):
            return "true"
        if val_lower in ("0", "false", "no", "off", "inactive"):
            return "false"
        raise ValueError(
            f"Invalid outOfService value '{raw_value}'. Expected true/false or 1/0."
        )

    if property_name == "presentValue":
        if object_type in BINARY_OBJECT_TYPES:
            if val_lower in ("1", "true", "on", "active"):
                return "active"
            if val_lower in ("0", "false", "off", "inactive"):
                return "inactive"
            raise ValueError(
                f"Invalid binary value '{raw_value}' for {object_type}. "
                "Expected active/inactive, on/off, true/false, or 1/0."
            )
        if object_type in MULTISTATE_OBJECT_TYPES:
            try:
                int_val = int(val_str)
                if int_val < 1:
                    raise ValueError
                return str(int_val)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid multi-state value '{raw_value}' for {object_type}. "
                    "Expected a positive integer (1..N) or 'null'."
                ) from exc
        if object_type in ANALOG_OBJECT_TYPES:
            try:
                float(val_str)
                return val_str
            except ValueError as exc:
                raise ValueError(
                    f"Invalid numeric value '{raw_value}' for {object_type}. "
                    "Expected a number or 'null'."
                ) from exc

    return val_str


def find_single_device(bacnet, device_ip):
    """Finds a single BACnet device at a known IP/BACnet address."""
    print(f"Sending targeted Who-Is to {device_ip}...")
    bacnet.whois_router_to_network(network=None, destination=device_ip)
    bacnet.whois(device_ip, global_broadcast=True)
    time.sleep(2)

    for entry in getattr(bacnet, "devices", []) or []:
        if len(entry) == 4:
            name, manufacturer, address, instance_id = entry
        else:
            address, instance_id = entry[0], entry[1]
            name, manufacturer = f"Unknown_{instance_id}", "Unknown"
        if str(address).startswith(device_ip):
            print(f"Device responded. Address: {address}, Instance ID: {instance_id}")
            return [(name, manufacturer, address, instance_id)]

    return []


def resolve_target_device(
    bacnet,
    target_ip="",
    target_device_id="",
    subnet_broadcast="",
    networks="",
    global_scan=False,
):
    """Resolves the target device address and BACnet device ID."""
    device_id_int = int(target_device_id) if str(target_device_id).strip() != "" else None

    if target_ip and device_id_int is not None:
        return target_ip, device_id_int, None

    if target_ip and device_id_int is None:
        matches = find_single_device(bacnet, target_ip)
        if matches:
            name, _, address, resolved_id = matches[0]
            return address, int(resolved_id), name
        return target_ip, None, None

    if device_id_int is not None:
        print(f"Discovering BACnet device with ID {device_id_int}...")
        if subnet_broadcast:
            bacnet.whois(
                f"{subnet_broadcast} {device_id_int} {device_id_int}",
                global_broadcast=True,
            )
            time.sleep(3)
        elif networks:
            bacnet_networks = string_to_integer_list(networks)
            bacnet.discover(
                global_broadcast=global_scan,
                limits=(device_id_int, device_id_int),
                networks=bacnet_networks,
            )
        else:
            bacnet.discover(
                global_broadcast=global_scan,
                limits=(device_id_int, device_id_int),
            )

        for entry in getattr(bacnet, "devices", []) or []:
            if len(entry) == 4:
                name, _, address, instance_id = entry
            else:
                address, instance_id = entry[0], entry[1]
                name = f"Unknown_{instance_id}"
            if int(instance_id) == device_id_int:
                print(f"Found device {device_id_int} at address {address} ({name}).")
                return address, device_id_int, name

        raise RuntimeError(
            f"Could not discover BACnet device with ID {device_id_int} on the network."
        )

    raise ValueError(
        "Either --ip (-i) or --bacnetid (-b) must be provided to identify the target device."
    )


def resolve_point_by_name(bacnet, device_address, device_id, point_name):
    """Resolves a BACnet point objectName to (object_type, object_instance, resolved_name)."""
    if device_id is None:
        matches = find_single_device(bacnet, device_address)
        if matches:
            device_id = int(matches[0][3])
        else:
            raise RuntimeError(
                f"Cannot resolve point name '{point_name}' without a BACnet device ID "
                f"(device at {device_address} did not respond to Who-Is). "
                "Specify --bacnetid (-b) or use --object (-o) <objectType>:<instance>."
            )

    target_name = point_name.strip()
    print(
        f"Searching for point '{target_name}' on device {device_id} at {device_address}..."
    )

    try:
        dev = BAC0.device(device_address, device_id, bacnet, poll=0)
        for pt in getattr(dev, "points", []) or []:
            pt_name = str(getattr(pt.properties, "name", "")).strip()
            if pt_name == target_name or pt_name.lower() == target_name.lower():
                obj_type = normalize_object_type(getattr(pt.properties, "type", ""))
                obj_inst = int(getattr(pt.properties, "address", 0))
                return obj_type, obj_inst, pt_name
    except Exception as e:
        print(f"Note: BAC0.device lookup fallback to objectList enumeration ({e})")

    object_list = bacnet.read(f"{device_address} device {device_id} objectList")
    if not isinstance(object_list, list) or not object_list:
        raise RuntimeError(
            f"Could not read objectList from device {device_id} at {device_address}."
        )

    case_insensitive_match = None
    for item in object_list:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        obj_type, obj_inst = item[0], item[1]
        if not isinstance(obj_type, str) or obj_type == "device":
            continue
        try:
            obj_name = bacnet.read(
                f"{device_address} {obj_type} {obj_inst} objectName"
            )
            if obj_name is None:
                continue
            obj_name_str = str(obj_name).strip()
            if obj_name_str == target_name:
                return normalize_object_type(obj_type), int(obj_inst), obj_name_str
            if (
                case_insensitive_match is None
                and obj_name_str.lower() == target_name.lower()
            ):
                case_insensitive_match = (
                    normalize_object_type(obj_type),
                    int(obj_inst),
                    obj_name_str,
                )
        except Exception:
            continue

    if case_insensitive_match is not None:
        return case_insensitive_match

    raise RuntimeError(
        f"Point '{target_name}' not found on device {device_id} ({device_address})."
    )


def safe_read_property(bacnet, address, obj_type, obj_inst, prop_name):
    """Reads a BACnet property, returning None if unreadable."""
    try:
        return bacnet.read(f"{address} {obj_type} {obj_inst} {prop_name}")
    except Exception:
        return None


def write_point_value(
    bacnet,
    device_address,
    object_type,
    object_instance,
    value,
    property_name="presentValue",
    priority=None,
    device_id=None,
    point_name=None,
):
    """Reads current point value, writes the new value via BAC0, and reads back the updated value."""
    obj_type = normalize_object_type(object_type)
    obj_inst = int(object_instance)
    formatted_val = format_write_value(value, obj_type, property_name)

    if not point_name:
        point_name = safe_read_property(
            bacnet, device_address, obj_type, obj_inst, "objectName"
        )
    units = safe_read_property(bacnet, device_address, obj_type, obj_inst, "units")
    before_value = safe_read_property(
        bacnet, device_address, obj_type, obj_inst, property_name
    )

    if priority is not None:
        prio_int = int(priority)
        if not 1 <= prio_int <= 16:
            raise ValueError(
                f"Invalid BACnet write priority {priority}. Must be between 1 and 16."
            )
        write_cmd = (
            f"{device_address} {obj_type} {obj_inst} "
            f"{property_name} {formatted_val} - {prio_int}"
        )
    else:
        prio_int = None
        write_cmd = (
            f"{device_address} {obj_type} {obj_inst} "
            f"{property_name} {formatted_val}"
        )

    print(f"Executing BACnet write: {write_cmd}")
    bacnet.write(write_cmd)

    after_value = safe_read_property(
        bacnet, device_address, obj_type, obj_inst, property_name
    )

    result = {
        "device_address": device_address,
        "device_id": device_id if device_id is not None else "",
        "point_name": point_name if point_name is not None else "",
        "object": f"{obj_type}:{obj_inst}",
        "property": property_name,
        "priority": prio_int if prio_int is not None else "default",
        "previous_value": before_value,
        "written_value": formatted_val,
        "current_value": after_value,
        "units": units if units is not None else "",
    }
    return result


def build_parser():
    parser = argparse.ArgumentParser(
        description="Write a value to a specific BACnet point on a target device using BAC0."
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="increase the verbosity level (optional)",
    )
    parser.add_argument(
        "-a",
        "--address",
        default="",
        help="IP address of local BACnet interface (optional)",
    )
    parser.add_argument(
        "-i",
        "--ip",
        default="",
        help="IP or BACnet address of the target BACnet device (e.g. 192.168.1.50 or 2001:4)",
    )
    parser.add_argument(
        "-b",
        "--bacnetid",
        default="",
        help="BACnet device ID of the target device (e.g. 12345)",
    )
    parser.add_argument(
        "-o",
        "--object",
        default="",
        help="target BACnet object as <objectType>:<instance> (e.g. analogValue:1, binaryOutput:2)",
    )
    parser.add_argument(
        "--instance",
        default="",
        help="target BACnet object instance number (optional if specified in --object)",
    )
    parser.add_argument(
        "-p",
        "--point",
        default="",
        help="target BACnet point objectName (alternative to --object)",
    )
    parser.add_argument(
        "-w",
        "--value",
        required=True,
        help="value to write to the point (use 'null' or 'release' to relinquish a priority)",
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=None,
        help="BACnet write priority (1-16, optional)",
    )
    parser.add_argument(
        "--property",
        default="presentValue",
        help="BACnet property to write (default: presentValue)",
    )
    parser.add_argument(
        "-n",
        "--networks",
        default="",
        help="comma separated target list of BACnet networks for device discovery (optional)",
    )
    parser.add_argument(
        "-g",
        "--globalscan",
        action="store_true",
        default=False,
        help="use global broadcast when discovering target device by BACnet ID (optional)",
    )
    parser.add_argument(
        "-s",
        "--subnet_broadcast",
        default="",
        help="subnet broadcast address when discovering target device by BACnet ID (optional)",
    )
    return parser


def main(argv=None):
    show_title()

    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.ip and not args.bacnetid:
        parser.error("At least one of --ip (-i) or --bacnetid (-b) must be specified.")

    if not args.object and not args.point:
        parser.error("Either --object (-o) or --point (-p) must be specified.")

    if args.verbose:
        print("program arguments:")
        print(args)
        BAC0.log_level("info")
    else:
        BAC0.log_level("silence")

    print("Initializing BAC0 client...")
    bacnet = None
    try:
        if args.address != "":
            bacnet = BAC0.lite(
                ip=args.address, deviceId=4194300, modelName="bacnet-write"
            )
        else:
            bacnet = BAC0.lite(deviceId=4194300, modelName="bacnet-write")
    except Exception as e:
        print(f"Failed to initialize BAC0 client: {e}")
        sys.exit(1)

    try:
        device_address, device_id, _ = resolve_target_device(
            bacnet,
            target_ip=args.ip,
            target_device_id=args.bacnetid,
            subnet_broadcast=args.subnet_broadcast,
            networks=args.networks,
            global_scan=args.globalscan,
        )

        if args.object:
            obj_type, obj_inst = parse_object_spec(args.object, args.instance)
            resolved_point_name = args.point or None
        else:
            obj_type, obj_inst, resolved_point_name = resolve_point_by_name(
                bacnet, device_address, device_id, args.point
            )

        result = write_point_value(
            bacnet=bacnet,
            device_address=device_address,
            object_type=obj_type,
            object_instance=obj_inst,
            value=args.value,
            property_name=args.property,
            priority=args.priority,
            device_id=device_id,
            point_name=resolved_point_name,
        )

        print(tabulate([result], headers="keys", tablefmt="psql"))
    finally:
        if bacnet is not None:
            try:
                bacnet.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nWrite aborted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\nWrite encountered a fatal error: {e}")
        sys.exit(1)

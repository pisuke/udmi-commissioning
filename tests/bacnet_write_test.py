#!/usr/bin/env python3

import importlib.util
import os
import sys
import types
import unittest
from unittest import mock

# Provide lightweight stubs if BAC0/pandas/tabulate are not installed in the active test runner
if "BAC0" not in sys.modules and importlib.util.find_spec("BAC0") is None:
    bac0_stub = types.ModuleType("BAC0")
    bac0_stub.lite = mock.MagicMock()
    bac0_stub.device = mock.MagicMock()
    bac0_stub.log_level = mock.MagicMock()
    sys.modules["BAC0"] = bac0_stub

if "tabulate" not in sys.modules and importlib.util.find_spec("tabulate") is None:
    tabulate_stub = types.ModuleType("tabulate")
    tabulate_stub.tabulate = lambda df, **kwargs: str(df)
    sys.modules["tabulate"] = tabulate_stub

SCRIPT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "bacnet-write.py")
)
spec = importlib.util.spec_from_file_location("bacnet_write", SCRIPT_PATH)
bacnet_write = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bacnet_write)


class TestBacnetWrite(unittest.TestCase):

    def test_parse_object_spec(self):
        self.assertEqual(
            bacnet_write.parse_object_spec("analogValue:1"),
            ("analogValue", 1),
        )
        self.assertEqual(
            bacnet_write.parse_object_spec("av:12"),
            ("analogValue", 12),
        )
        self.assertEqual(
            bacnet_write.parse_object_spec("bo:3"),
            ("binaryOutput", 3),
        )
        self.assertEqual(
            bacnet_write.parse_object_spec("msv", "4"),
            ("multiStateValue", 4),
        )
        self.assertEqual(
            bacnet_write.parse_object_spec("analogOutput 7"),
            ("analogOutput", 7),
        )
        with self.assertRaises(ValueError):
            bacnet_write.parse_object_spec("analogValue")

    def test_format_write_value(self):
        self.assertEqual(
            bacnet_write.format_write_value("21.5", "analogValue"),
            "21.5",
        )
        self.assertEqual(
            bacnet_write.format_write_value("release", "analogValue"),
            "null",
        )
        self.assertEqual(
            bacnet_write.format_write_value("1", "binaryValue"),
            "active",
        )
        self.assertEqual(
            bacnet_write.format_write_value("false", "binaryOutput"),
            "inactive",
        )
        self.assertEqual(
            bacnet_write.format_write_value("3", "multiStateValue"),
            "3",
        )
        with self.assertRaises(ValueError):
            bacnet_write.format_write_value("0", "multiStateValue")
        with self.assertRaises(ValueError):
            bacnet_write.format_write_value("not_a_number", "analogValue")

    def test_write_point_value_with_priority(self):
        fake_bacnet = mock.MagicMock()
        read_responses = {
            "192.168.1.50 analogValue 1 objectName": "ZN_T_SP",
            "192.168.1.50 analogValue 1 units": "degreesCelsius",
            "192.168.1.50 analogValue 1 presentValue": [20.0, 22.5],
        }

        def fake_read(cmd):
            val = read_responses.get(cmd)
            if isinstance(val, list):
                return val.pop(0)
            return val

        fake_bacnet.read.side_effect = fake_read

        res = bacnet_write.write_point_value(
            bacnet=fake_bacnet,
            device_address="192.168.1.50",
            object_type="av",
            object_instance=1,
            value="22.5",
            priority=8,
            device_id=1234,
        )

        fake_bacnet.write.assert_called_once_with(
            "192.168.1.50 analogValue 1 presentValue 22.5 - 8"
        )
        self.assertEqual(res["point_name"], "ZN_T_SP")
        self.assertEqual(res["previous_value"], 20.0)
        self.assertEqual(res["written_value"], "22.5")
        self.assertEqual(res["current_value"], 22.5)
        self.assertEqual(res["priority"], 8)

    def test_resolve_point_by_name_object_list_fallback(self):
        fake_bacnet = mock.MagicMock()

        def fake_read(cmd):
            if cmd == "192.168.1.50 device 1234 objectList":
                return [("device", 1234), ("analogValue", 5), ("binaryValue", 2)]
            if cmd == "192.168.1.50 analogValue 5 objectName":
                return "Room_Temp_Setpoint"
            if cmd == "192.168.1.50 binaryValue 2 objectName":
                return "Occupancy_Cmd"
            return None

        fake_bacnet.read.side_effect = fake_read

        with mock.patch.object(
            bacnet_write.BAC0, "device", side_effect=RuntimeError("no device model")
        ):
            obj_type, obj_inst, resolved_name = bacnet_write.resolve_point_by_name(
                fake_bacnet, "192.168.1.50", 1234, "room_temp_setpoint"
            )

        self.assertEqual(obj_type, "analogValue")
        self.assertEqual(obj_inst, 5)
        self.assertEqual(resolved_name, "Room_Temp_Setpoint")

    def test_main_end_to_end(self):
        fake_bacnet = mock.MagicMock()
        fake_bacnet.devices = [("Controller_1", "VendorX", "192.168.1.50", 1234)]
        fake_bacnet.read.side_effect = [
            "ZN_T_SP",
            "degreesCelsius",
            21.0,
            23.0,
        ]

        with mock.patch.object(bacnet_write.BAC0, "lite", return_value=fake_bacnet):
            bacnet_write.main(
                [
                    "-b",
                    "1234",
                    "-o",
                    "analogValue:1",
                    "-w",
                    "23.0",
                    "--priority",
                    "8",
                ]
            )

        fake_bacnet.discover.assert_called_once_with(
            global_broadcast=False, limits=(1234, 1234)
        )
        fake_bacnet.write.assert_called_once_with(
            "192.168.1.50 analogValue 1 presentValue 23.0 - 8"
        )
        fake_bacnet.disconnect.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)

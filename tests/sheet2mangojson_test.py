#!/usr/bin/env python3

import unittest
import subprocess
import json
import os
import shutil

# --- Configuration ---
MAIN_SCRIPT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sheet2mangojson.py'))
INPUT_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'sheet2mangojson_test_scan.xlsx'))
BASELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'baselines'))
TEMP_OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'temp_output'))

class TestSheet2Mango(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Create required directories before testing."""
        os.makedirs(BASELINE_DIR, exist_ok=True)
        os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)
        
        if not os.path.exists(INPUT_FILE):
            raise FileNotFoundError(f"Test requires {INPUT_FILE} to exist.")

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary outputs after tests run."""
        if os.path.exists(TEMP_OUTPUT_DIR):
            shutil.rmtree(TEMP_OUTPUT_DIR)

    def sanitize_publisher_json(self, json_data):
        """Removes dynamically generated RSA keys to allow for deterministic testing."""
        if "publishers" in json_data:
            for pub in json_data["publishers"]:
                if "privateKey" in pub:
                    pub["privateKey"] = "<DYNAMIC_KEY_REMOVED_FOR_TEST>"
                if "publicKey" in pub:
                    pub["publicKey"] = "<DYNAMIC_KEY_REMOVED_FOR_TEST>"
        return json_data

    def run_and_compare(self, version):
        """Runs the main script, normalizes output, and compares against baselines."""
        output_prefix = os.path.join(TEMP_OUTPUT_DIR, f"test_v{version.replace('.*', '')}")
        
        # 1. Run the CLI command
        cmd = [
            "python3", MAIN_SCRIPT,
            "-i", INPUT_FILE,
            "-o", output_prefix,
            "--udmi-version", version,
            "--unique", 
            "--ds-enabled", "True"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed for version {version}: {result.stderr}")

        # 2. Check generated files
        bacnet_file = f"{output_prefix}_bacnet_config.json"
        udmi_file = f"{output_prefix}_udmi_publisher.json"
        
        self.assertTrue(os.path.exists(bacnet_file), "BACnet config was not generated.")
        self.assertTrue(os.path.exists(udmi_file), "UDMI publisher config was not generated.")

        # 3. Load and sanitize JSONs
        with open(bacnet_file, 'r') as f:
            generated_bacnet = json.load(f)
            
        with open(udmi_file, 'r') as f:
            generated_udmi = self.sanitize_publisher_json(json.load(f))

        # 4. Compare or Record Baselines
        baseline_bacnet_file = os.path.join(BASELINE_DIR, f"baseline_v{version.replace('.*', '')}_bacnet.json")
        baseline_udmi_file = os.path.join(BASELINE_DIR, f"baseline_v{version.replace('.*', '')}_udmi.json")

        # If baselines don't exist, we create them (Record Mode)
        if not os.path.exists(baseline_bacnet_file) or not os.path.exists(baseline_udmi_file):
            print(f"\n[RECORD MODE] Creating new baselines for version {version}...")
            with open(baseline_bacnet_file, 'w') as f:
                json.dump(generated_bacnet, f, indent=4)
            with open(baseline_udmi_file, 'w') as f:
                json.dump(generated_udmi, f, indent=4)
            return # Skip comparison since we just made the baseline

        # Load baselines and compare
        with open(baseline_bacnet_file, 'r') as f:
            baseline_bacnet = json.load(f)
        with open(baseline_udmi_file, 'r') as f:
            baseline_udmi = json.load(f)

        self.assertDictEqual(generated_bacnet, baseline_bacnet, f"BACnet JSON drift detected in {version}!")
        self.assertDictEqual(generated_udmi, baseline_udmi, f"UDMI JSON drift detected in {version}!")

    def test_version_5_3(self):
        self.run_and_compare("5.3.*")

    def test_version_5_4(self):
        self.run_and_compare("5.4.*")

    def test_version_5_5(self):
        self.run_and_compare("5.5.*")

if __name__ == "__main__":
    unittest.main(verbosity=2)
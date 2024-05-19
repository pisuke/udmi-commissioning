## UDMI commissioning tool

The aim of the UDMI commissioning tool is to compare the values 
of data points from the local network (BACnet or Modbus values)
to the values appearing in GCP PubSub.

The UDMI commissioning tool accepts an input file with a list
of BACnet object names or Modbus registers numbers and a list
of UDMI device and point names, and validates that their values
are the same.

### Installation

This tool depends on the BAC0 python library, which has been
tested to work well with Python 3.9. It is therefore recommended
to create a Python 3.9 virtual environment first:

```
python3.9 -m venv py39
```

and then activate this environment

```
source py39/bin/activate
```

After this, install the python library dependencies:

```
python3 -m pip install -r requirements.txt
```

### Use



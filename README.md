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
tested to work well with Python 3.9 and Python 3.10. 
It is therefore recommended to create a Python 3.9 or 3.10 
virtual environment first:

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


#### Input file generation from BACnet scan

The first step is to generate a UDMI validation input file.
This can be achieved by running the `bacnet-scan.py` tool.

The `bacnet-scan` help is shown below.

```
% ./bacnet-scan.py -h
 ____    _    ____            _
| __ )  / \  / ___|_ __   ___| |_      ___  ___ __ _ _ __
|  _ \ / _ \| |   | '_ \ / _ \ __|____/ __|/ __/ _` | '_ \
| |_) / ___ \ |___| | | |  __/ ||_____\__ \ (_| (_| | | | |
|____/_/   \_\____|_| |_|\___|\__|    |___/\___\__,_|_| |_|


usage: bacnet-scan.py [-h] [-v] [-x EXPORT] [-a ADDRESS]

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         increase the verbosity level
  -x EXPORT, --export EXPORT
                        spreadsheet file name for scan results (optional), supported extensions are .xlsx and .ods
  -a ADDRESS, --address ADDRESS
                        IP address of BACnet interface (optional)
```

An example command line invocation of the `bacnet-scan.py` tool is the following:

```
./bacnet-scan.py -x bacnet-scan-output.xls
```

If needed, specify the IP address of the network device that is connected to the BACnet IP network (for instance 
192.168.1.100 in the example below):

```
./bacnet-scan.py -x bacnet-scan-output.xls -a 192.168.1.100
```

This process generates an output `.xlsx` file that should be used as input for the next process.

See below the content of the output `.xlsx` file.

![BACnet scan output](img/bacnet-scan-output.png)

Other spreadsheet format and extensions that can be used are `.xls` and `.ods`.

#### Addition of cloud point names

The output file of the previous step should be used as the input file of the `udmi-commissioning.py` tool.

For this purpose, open it in a spreadsheet program like Excel, LibreOffice or Google Sheets and add the cloud device ID 
and cloud point name, as shown in the image below.

![UDMI commissioning input file](img/input.png)

Once the file includes all the target points needed for commissioning validation, invoke the `udmi-commissioning.py` tool:

```
./udmi-commissioning.py -p GCP-PROJECT-NAME -s GCP-PUBSUB-SUBSCRIPTION -i input.xlsx -o output.xls
```

More options for `udmi-commissioning.py` are shown below:

```
% ./udmi-commissioning.py -h
 _   _ ____  __  __ ___
| | | |  _ \|  \/  |_ _|
| | | | | | | |\/| || |
| |_| | |_| | |  | || |
 \___/|____/|_|  |_|___|


                               _         _             _
  ___ ___  _ __ ___  _ __ ___ (_)___ ___(_) ___  _ __ (_)_ __   __ _
 / __/ _ \| '_ ` _ \| '_ ` _ \| / __/ __| |/ _ \| '_ \| | '_ \ / _` |
| (_| (_) | | | | | | | | | | | \__ \__ \ | (_) | | | | | | | | (_| |
 \___\___/|_| |_| |_|_| |_| |_|_|___/___/_|\___/|_| |_|_|_| |_|\__, |
                                                               |___/

usage: udmi-commissioning.py [-h] [-v] [-p PROJECT] [-s SUB] [-i INPUT] [-o OUTPUT] [-a ADDRESS] [-t TIMEOUT]

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         increase the verbosity level
  -p PROJECT, --project PROJECT
                        GCP project id (required)
  -s SUB, --sub SUB     GCP PubSub subscription (required)
  -i INPUT, --input INPUT
                        input file containing the point list (optional, the default is input.xlsx, accepted extensions are .xlsx and .ods)
  -o OUTPUT, --output OUTPUT
                        sheet file name for output results (optional, the default is output.xlsx, accepted extensions are .xlsx and .ods)
  -a ADDRESS, --address ADDRESS
                        IP address of BACnet interface (optional)
  -t TIMEOUT, --timeout TIMEOUT
                        time interval in seconds for which to receive messages (optional, default=3600 seconds, equating to 1 hour)
```

Leave the tool to run until it has discovered all the target points in GCP PubSub.
Once satisfied with the number of validated points, interrupt the program by pressing CTRL+C.
This key combination will be intercepted by the program to save the output file.

The content of the output spreadsheet file will now include the commissioning validation output, as shown below:

![UDMI commissioning validation output](img/output.png)

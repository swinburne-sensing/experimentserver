# experimentserver

**Warning:** This documentation should be used in tandem with the relevant safe operating procedures (SOPs) and risk assessments (RAs).

A tool for running hardware during experiments and logging results to a database.

## Installation

#### Software

The following packages were used during development:

- Python >= 3.7: https://www.python.org/
- NI-VISA >= 19.0: http://www.ni.com/en-au/support/downloads/drivers/download.ni-visa.html

NI-VISA is optional if pyvisa-py is used as a VISA backend.

#### Python Packages

See requirements.txt for a complete listing of Python modules (with versions) used in this project.

It is usually a good idea to run application code in a [virtual environment](https://realpython.com/python-virtual-environments-a-primer/) to avoid overwriting system packages with the specific versions used during development/testing of this software. This project provides pinned dependencies and as such some libraries may be behind current versions. Be aware of this when developing new modules as documentation and interfaces may significantly change between versions.

Fetch all package dependencies using pip:

``pip install -r requirements.txt``

If installed on Linux then users may need to be added to the dialout group to have write access to serial ports. Access can be granted using:

``sudo usermod -a -G dialout <username>``

#### udev rules

Linux requires additional udev rules to allow access to instruments via USB without root access. ``lsusb`` can be used to find the USB vendor and product IDs and an override in ``/etc/udev/rules.d/*.rules`` can be used to provide global access:

``SUBSYSTEM=="usb", ATTR{idVendor}=="<VENDOR_ID>", ATTR{idProduct}=="<PRODUCT_ID>", MODE="0666"``

Tested on Debian 10 (buster), attributes may vary by platform. 

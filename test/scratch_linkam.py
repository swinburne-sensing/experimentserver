import logging

from experimentserver.linkam.sdk import LinkamSDK

logging.basicConfig(level=logging.DEBUG)


sdk = LinkamSDK()
sdk.connect_usb()

print(1)

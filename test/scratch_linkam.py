import logging

from experimentserver.interface.linkam import LinkamSDK

logging.basicConfig(level=logging.DEBUG)


n = 0

while True:
    print(n)
    sdk = LinkamSDK()
    del sdk
    n += 1

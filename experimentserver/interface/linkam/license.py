import enum
import typing


_CHUNK_SIZE = 8


class LicenseOptions(enum.IntFlag):
    FLAG_OEM = 0x1,
    FLAG_EVALUATION = 0x2,
    FLAG_SDK = 0x4,
    FLAG_03 = 0x8,
    FLAG_04 = 0x10,
    FLAG_T95COMP = 0x20,
    FLAG_T95COMPMDSC = 0x40,
    FLAG_T95COMPTESTMOTION = 0x80,
    FLAG_T95COMPTESTPROFILES = 0x100,
    FLAG_09 = 0x200,
    FLAG_10 = 0x400,
    FLAG_11 = 0x800,
    FLAG_12 = 0x1000,
    FLAG_13 = 0x2000,
    FLAG_14 = 0x4000,
    FLAG_LINK = 0x8000,
    FLAG_LINKDV = 0x10000,
    FLAG_LINKMEA = 0x20000,
    FLAG_LINKIA = 0x40000,
    FLAG_LINKAF = 0x80000,
    FLAG_LINKFOC = 0x100000,
    FLAG_LINKCFR = 0x200000,
    FLAG_LINKTASC = 0x400000,
    FLAG_23 = 0x800000,
    FLAG_24 = 0x1000000,
    FLAG_25 = 0x2000000,
    FLAG_26 = 0x4000000,
    FLAG_27 = 0x8000000,
    FLAG_28 = 0x10000000,
    FLAG_29 = 0x20000000,
    FLAG_30 = 0x40000000,
    FLAG_31 = 0x80000000

    def get_key(self) -> typing.List[str]:
        value = ((self.value << 7) & 0x6000) | ((self.value << 5) & 0x0600) | \
                ((self.value << 3) & 0x0060) | ((self.value << 1) & 0x0006)

        return _license_hex_str(value.to_bytes(32, 'little')[::-1])


def _license_hex_str(data: bytes) -> typing.List[str]:
    # Construct hex string
    segments = [data[n:n + _CHUNK_SIZE] for n in range(0, len(data), _CHUNK_SIZE)]

    # Sum segments and convert to hex string
    return [f"{sum(x):04X}" for x in segments]


def fetch_license(license_file: typing.BinaryIO, host_key: typing.Optional[typing.List[str]] = None,
                  options: typing.Optional[LicenseOptions] = None):
    if options is None:
        options = LicenseOptions.FLAG_EVALUATION | LicenseOptions.FLAG_SDK

    options_key = options.get_key()

    if host_key is None:
        host_key = ['0000', '0000']

    # Initial use flag and initial use timer
    license_file.write(bytes((0 for _ in range(10))))

    # License key
    license_file.write('-'.join(host_key + options_key).encode('ascii'))

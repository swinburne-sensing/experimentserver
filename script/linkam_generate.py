import argparse
import binascii
import enum
import hashlib
import struct
import typing

import wmi


class KeyOptions(enum.IntFlag):
    OPTOEM = 0x1,
    OPTEVALUATION = 0x2,
    OPTSDK = 0x4,
    OPT03 = 0x8,
    OPT04 = 0x10,
    OPTT95COMP = 0x20,
    OPTT95COMPMDSC = 0x40,
    OPTT95COMPTESTMOTION = 0x80,
    OPTT95COMPTESTPROFILES = 0x100,
    OPT09 = 0x200,
    OPT10 = 0x400,
    OPT11 = 0x800,
    OPT12 = 0x1000,
    OPT13 = 0x2000,
    OPT14 = 0x4000,
    OPTLINK = 0x8000,
    OPTLINKDV = 0x10000,
    OPTLINKMEA = 0x20000,
    OPTLINKIA = 0x40000,
    OPTLINKAF = 0x80000,
    OPTLINKFOC = 0x100000,
    OPTLINKCFR = 0x200000,
    OPTLINKTASC = 0x400000,
    OPT23 = 0x800000,
    OPT24 = 0x1000000,
    OPT25 = 0x2000000,
    OPT26 = 0x4000000,
    OPT27 = 0x8000000,
    OPT28 = 0x10000000,
    OPT29 = 0x20000000,
    OPT30 = 0x40000000,
    OPT31 = 0x80000000,


def get_system_reg_str() -> str:
    # Get WMI objects
    wmi_interface = wmi.WMI()

    comp_manufacturer = None
    comp_model = None
    motherboard_make = None
    motherboard_type = None
    motherboard_serial = None
    cpu_make = None
    cpu_name = None
    cpu_id = None

    for payload in wmi_interface.query('SELECT * FROM Win32_ComputerSystem'):
        comp_manufacturer = payload.wmi_property('manufacturer').value
        comp_model = payload.wmi_property('model').value

    for payload in wmi_interface.query('SELECT * FROM Win32_BaseBoard'):
        motherboard_make = payload.wmi_property('Manufacturer').value
        motherboard_type = payload.wmi_property('Product').value
        motherboard_serial = payload.wmi_property('serialnumber').value

    for payload in wmi_interface.query('SELECT * FROM Win32_Processor'):
        cpu_make = payload.wmi_property('manufacturer').value
        cpu_name = payload.wmi_property('name').value
        cpu_id = payload.wmi_property('processorid').value

    return f"COMPUTER >> {comp_manufacturer}{comp_model} MBOARD >> {motherboard_make}{motherboard_type}{motherboard_serial} CPU >> {cpu_make}{cpu_name}{cpu_id}"


def get_system_reg_hash(system_reg: str) -> bytes:
    # Calculate SHA256 hash
    m = hashlib.sha256()
    m.update(system_reg.encode('ascii'))
    return m.digest()


def hash_to_chunks(system_hash: bytes):
    _CHUNK_SIZE = 8

    return [system_hash[n:n + _CHUNK_SIZE] for n in range(0, len(system_hash), _CHUNK_SIZE)]


def get_system_reg_hash_segment(system_reg_hash: bytes) -> typing.List[str]:
    # Construct hex string
    hex_seg = hash_to_chunks(system_reg_hash)

    # Sum segments and convert to hex string
    hex_seg = [f"{sum(x):04X}" for x in hex_seg]

    return hex_seg


def get_system_reg_key_segment(system_reg_hash: bytes) -> typing.List[str]:
    # Split into chunks
    system_hash_chunks = hash_to_chunks(system_reg_hash)
    system_hash_seg = get_system_reg_hash_segment(system_reg_hash)

    system_hash_seg = [struct.unpack('<H', binascii.unhexlify(x.encode())[::-1])[0] for x in system_hash_seg]

    # Cast to unsigned short
    # system_hash_chunks = [sum(x) for x in system_hash_chunks]

    # Work out "magic" value
    # (((int) num1 + (int) num3) / 3 * 5))
    # (((int) num2 + (int) num4) / 5 * 7))

    # Parse hash
    key_segment = []

    # 0D98-0D12
    return [f"{x:04X}" for x in [
        int(int((system_hash_seg[0] + system_hash_seg[2]) / 3) * 5) & 0xFFFF,
        int(int((system_hash_seg[1] + system_hash_seg[3]) / 5) * 7) & 0xFFFF,
    ]]


def decode_options(key_segment: str) -> int:
    # Parse value
    key_segment = int(key_segment, 16)

    return sum([
        ((key_segment & 0x6000) >> 7),
        ((key_segment & 0x0600) >> 5),
        ((key_segment & 0x0060) >> 3),
        ((key_segment & 0x0006) >> 1),
    ]) & 0xFF


def get_key_options(key: str) -> KeyOptions:
    key_segments = key.split('-')
    key_segments = [decode_options(x) for x in key_segments[2:]]

    return KeyOptions(key_segments[0] << 24 | key_segments[1] << 16 | key_segments[2] << 8 | key_segments[3])


def generate_key_options(options: KeyOptions) -> typing.List[str]:
    value = ((options.value << 7) & 0x6000) | ((options.value << 5) & 0x0600) | \
            ((options.value << 3) & 0x0060) | ((options.value << 1) & 0x0006)

    return get_system_reg_hash_segment(value.to_bytes(32, 'little')[::-1])


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('target', help='Output filename')

    app_args = parser.parse_args()

    options_lab = get_key_options('0E1A-0B28-9888-8189-1109-1831')
    options1 = get_key_options('D191-5A72-0919-9091-9008-9925')
    options2 = get_key_options('D191-5A72-0000-0000-0000-0024')

    print(f"0x{options1.value:08X} = {options1!r}")
    print(f"0x{options2.value:08X} = {options2!r}")

    options3 = generate_key_options(KeyOptions.OPTSDK)

    print(f"{'-'.join(options3)}")

    # OK
    reg_str = get_system_reg_str()

    # OK
    reg_hash_byte = get_system_reg_hash(reg_str)

    # OK
    reg_hash_seg = get_system_reg_hash_segment(reg_hash_byte)

    # Expected 0D98-0D12, got 0EB0-074D
    reg_key = get_system_reg_key_segment(reg_hash_byte)

    print(reg_str)
    print(reg_hash_byte)
    print('-'.join(reg_hash_seg))
    print('-'.join(reg_key + options3))

    print(1)


if __name__ == '__main__':
    main()

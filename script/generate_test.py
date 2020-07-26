stages = 4 * [10] + 6 * [(x + 1) * 40 for x in range(5)] + 8 * [200]
exposure = '3 min'
recover = '45 min'

for conc in stages:
    print(f"  - !setup mfc_air_dry, set_flow_rate, {200 - conc}sccm")
    print(f"  - !setup mfc_hydrogen, set_flow_rate, {conc}sccm")
    print(f"  - !delay_tag {exposure}, phase=expose")
    print(f"  - !setup mfc_air_dry, set_flow_rate, 200sccm")
    print(f"  - !setup mfc_hydrogen, set_flow_rate, 0sccm")
    print(f"  - !delay_tag {recover}, phase=recover")
    print()

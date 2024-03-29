stages = 40 * [(x + 1) * 40 for x in range(5)]
exposure = '15 s'
recover = '1 min'

t = 0

print(f"  # Without zero")

for conc in stages:
    print(f"  - !setup mfc_air_dry, set_flow_rate, {200 - conc}sccm")
    print(f"  - !setup mfc_carbon_dioxide, set_flow_rate, {conc}sccm")
    print(f"  - !delay_tag {exposure}, phase=expose")
    print(f"  - !setup mfc_air_dry, set_flow_rate, 200sccm")
    print(f"  - !setup mfc_carbon_dioxide, set_flow_rate, 0sccm")
    print(f"  - !delay_tag {recover}, phase=recover")
    print()

print(f"  # With zero")

for conc in stages:
    if t == 0:
        print(f"  - !setup bridgeanalyzer, zero")
        print(f"  - !delay_tag 1min, phase=setup")
        print()

    print(f"  - !setup mfc_air_dry, set_flow_rate, {200 - conc}sccm")
    print(f"  - !setup mfc_carbon_dioxide, set_flow_rate, {conc}sccm")
    print(f"  - !delay_tag {exposure}, phase=expose")
    t += 15
    print(f"  - !setup mfc_air_dry, set_flow_rate, 200sccm")
    print(f"  - !setup mfc_carbon_dioxide, set_flow_rate, 0sccm")
    print(f"  - !delay_tag {recover}, phase=recover")
    t += 60
    print()

    if t > (30*60):
        t = 0

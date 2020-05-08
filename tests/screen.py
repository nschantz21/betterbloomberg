from betterbloomberg.screen import EQS

results = EQS(
    "Global Volume Surges",
    screen_type="GLOBAL",
    group="Global Emerging Markets",
    date="20100101")
print(results.data)
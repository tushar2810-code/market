from nsepython import nse_quote
import json

data = nse_quote('RELIANCE')
with open('.tmp/reliance_nse.json', 'w') as f:
    json.dump(data, f, indent=2)
print("Done")

# for all *.bin files in current dir, fetch the data from https://api.mainnet-beta.solana.com/

import requests
import os
import time
import base64
accounts = []
for file in os.listdir("."):
    if file.endswith(".bin"):
        if os.path.getsize(file) > 0:
            print(f"{file} is not empty. skipping...")
            continue
        accounts.append(file.split(".")[0])

'''
curl https://api.mainnet-beta.solana.com -s -X \
  POST -H "Content-Type: application/json" -d '
  {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": [
      "vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg",
      {
        "encoding": "base58"
      }
    ]
  }
'
'''
url = "https://api.mainnet-beta.solana.com"
for account in accounts:
    data = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [account, {"encoding": "base64"}]
    }
    response = requests.post(url, json=data)
    data = base64.b64decode(response.json()["result"]["value"]["data"][0])
    print(f"{account} {len(data)}")
    with open(f"{account}.bin", "wb") as f:
        f.write(data)
    # sleep 0.5 seconds
    time.sleep(0.5)

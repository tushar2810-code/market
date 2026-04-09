import requests
import json
import hashlib

CLIENT_ID    = 'FN176274_U' 
API_SECRET   = 'qLiXlxMB8BNXYJ0bLe2iME8jJkHHjAhmh0RaKgioZ3efrIZAhKh7YrEQLXcnNOmx'

auth_code = input("PASTE YOUR BRAND NEW OAUTH CODE HERE AND PRESS ENTER: ").strip()

checksum_string = CLIENT_ID + API_SECRET + auth_code
checksum = hashlib.sha256(checksum_string.encode('utf-8')).hexdigest()

payload = {'code': auth_code, 'checksum': checksum}
data = 'jData=' + json.dumps(payload)
headers = {'Content-Type': 'application/x-www-form-urlencoded'}

print("\nSending to Shoonya...")
res = requests.post('https://trade.shoonya.com/NorenWClientAPI/GenAcsTok', data=data, headers=headers)
print('\n--- SHOONYA RESPONSE ---')
print(res.text)
print('------------------------\n')
if "susertoken" in res.text:
    print("SUCCESS! YOU GOT THE TOKEN!")

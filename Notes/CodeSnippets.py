import json
import hashlib


# https://www.geeksforgeeks.org/with-statement-in-python/ 

json_string = {
    "key": "what's up?",
    "key2": {
        "key2.1": "moin",
        "key2.2": "hello"
    }
}

# Create or open an existing file and write a dict in the file
with open('json_data.json', 'w') as outfile:
    json.dump(json_string, outfile)


# Open a file and read the dict
with open('json_data.json') as json_file:
    data = json.load(json_file)
    print(data)


# Example of sha or md5 encryption
md5 = hashlib.md5("0-0795027408".encode())
sha = hashlib.sha256("0-0795027408".encode())
print(md5.hexdigest())
print(sha.hexdigest())


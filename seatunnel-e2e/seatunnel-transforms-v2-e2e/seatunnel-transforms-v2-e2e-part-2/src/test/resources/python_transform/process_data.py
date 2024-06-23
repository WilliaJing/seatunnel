import re
import json
import sys

def process_data(input_data):
    email = input_data[3].strip()
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
       input_data[3] = ""

if __name__ == "__main__":
    # Example input data as List<String>
#     input_data = [3, "Jane", 23, "123qq.com"]
    input_data = json.loads(sys.stdin.read())
    process_data(input_data)
    print(json.dumps(input_data))

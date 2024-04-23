import os
import time
from pydaos import DCont, DDict

# Create a DAOS container
daos_cont = DCont("pydaos", "kvstore", None)

# Create a DAOS dictionary or get it if it already exists
try:
    daos_dict = daos_cont.get("pydaos_kvstore_dict")
except:
    daos_dict = daos_cont.dict("pydaos_kvstore_dict")

# Directory to store uploaded files
upload_dir = "uploads"
os.makedirs(upload_dir, exist_ok=True)

# Size of chunks in MB
n = int(input("Enter size of chunks (in MB): "))
CHUNK_SIZE = n * 1024 * 1024

# Function to print help
def print_help():
    print("?\t- Print this help")
    print("r\t- Read a key")
    print("u\t- Upload file for a new key")
    print("p\t- Display keys")
    print("q\t- Quit")
def read_key():
  try:
    key = input("Enter key to read: ")
    chunk_count = 0
    assembled_data = b""

    start_time = time.time()

    # Construct an initial list of potential chunk keys (adjustable based on knowledge)
    chunk_keys = []
    for i in range(chunk_count):
      chunk_keys.append(f"{key}chunk{i}")

    # Retrieve chunks in bulk using bget
    chunk_data = daos_dict.bget(chunk_keys)

    # Check if the retrieved data is a list and convert it to a dictionary if needed
    if isinstance(chunk_data, list):
      chunk_data_dict = {}
      for i in range(len(chunk_keys)):
        chunk_data_dict[chunk_keys[i]] = chunk_data[i]
    else:
      chunk_data_dict = chunk_data

    for chunk_key, chunk_data in chunk_data_dict.items():
      assembled_data += chunk_data

    # Rest of the code for checking retrieved chunks and processing data...

  except KeyError:
    print("\tError! Key not found")
  except Exception as e:
    print(f"An error occurred during read: {e}")

def save_value_as_file(key, value):
    filename = os.path.join(upload_dir, f"{key}.dat")
    with open(filename, "wb") as f:
        f.write(value)
    print(f"Value saved as file: {filename}")

# Function to print all keys
def print_keys():
    for i in daos_dict:
        print(i)

# Function to upload file for a new key with time measurement
def upload_file():
    key = input("Enter new key: ")
    file_path = input("Enter path to file: ")

    if os.path.exists(file_path):
        chunk_dict = {}
        try:
            start_time = time.time()
            with open(file_path, "rb") as f:
                chunk_count = 0
                while True:
                    data = f.read(CHUNK_SIZE)
                    if not data:
                        break
                    chunk_key = f"{key}chunk{chunk_count}"
                    chunk_dict[chunk_key] = data
                    chunk_count += 1
            # Measure time only for the bput operation
            bput_start_time = time.time()
            daos_dict.bput(chunk_dict)
            bput_end_time = time.time()
            upload_time = bput_end_time - bput_start_time

            print(f"File uploaded in {chunk_count} chunks successfully. Time taken: {upload_time} seconds")
        except Exception as e:
            print(f"Error uploading file: {e}")
    else:
        print("File not found.")

# Main loop
while True:
    print("\nCommands:")
    print_help()
    cmd = input("Enter command (? for help): ")

    if cmd == "?":
        print_help()
    elif cmd == "r":
        read_key()
    elif cmd == "u":
        upload_file()
    elif cmd == "p":
        print_keys()
    elif cmd == "q":
        break
    else:
        print("Invalid command. Enter '?' for help.")

print("Program ended.")

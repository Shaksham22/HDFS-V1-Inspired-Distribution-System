import socket
import time
import os
import json
import warnings
import threading
from nodespacevirtualizer import check_folder_size


filepath=os.path.dirname(os.path.abspath(__file__))
blockdata=[]

MAX_RETRIES = 3
RETRY_INTERVAL = 300  # 5 minutes in seconds

def transmitfile(filename,partitionnumber, server_address, server_port,partitiondata,targetnodes):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((server_address, server_port))
    partitioninfo=[filename,partitionnumber,partitiondata,targetnodes]
    client.sendall((json.dumps(partitioninfo)).encode('utf-8'))
    client.close()
    print(filename+" patition "+str(partitionnumber)+" sent successfully.")

def blockreceiver(server_address, server_port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((server_address, server_port))
    server.listen(1)  # Listen for incoming connections

    print(f"Blockreceiver listening on {server_address}:{server_port}")

    while True:
        client_socket, client_address = server.accept()
        print(f"Connection established with {client_address}")
        blockinfo = b""

        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            blockinfo += data

        if not blockinfo:
            break

        blockinfo = json.loads(blockinfo.decode('utf-8'))
        print(type(blockinfo[0]), type(blockinfo[1]), type(blockinfo[2]), type(blockinfo[3]))
    
        with open(filepath + "/blocklist/" + blockinfo[0] + str(blockinfo[1]), 'w') as file:
            file.write(blockinfo[2])
        

        print("File received successfully.")

        if len(blockinfo[3]) != 0:
            address = blockinfo[3].pop(0)
            print(blockinfo[0], blockinfo[1], blockinfo[3])
            transmitfile(blockinfo[0], blockinfo[1], address[1][0], address[1][1], blockinfo[2], blockinfo[3])
            print("Block replication initiated")

        client_socket.close()



def load_client_id():
    try:
        with open(filepath+"/nodeinfo.json", "r") as file:
            client_info = file.read()
            if(client_info!=None):
                client_info=json.loads(client_info)
                return client_info
            else:
                return(None)
    except FileNotFoundError:
        return None

def save_client_id(client_id,assignedport):
    info={"nodeid":client_id,"nodeport":assignedport}
    with open(filepath+"/nodeinfo.json", "w") as file:
        file.write(json.dumps(info))

def send_request_for_id(server_address):
    retries = 0

    while retries <= MAX_RETRIES:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(server_address)

        try:
            # Send a request for a new login (client ID)
            request_message = ["N001"]
            client.send((json.dumps(request_message)).encode('utf-8'))
            print("No client ID found. A new client ID requested")

            # Receive the server's response with the confirmation code and assigned client ID
            response = json.loads((client.recv(1024)).decode('utf-8'))
            try:
                confirmation_code=response[0]
                assigned_id = response[1]
                assignedport=response[2]
            except Exception as e:
                print(f"Error during verification of confirmation code {e}")

            if confirmation_code == "A001":
                print(f"New ID assigned: {assigned_id}","port ",assignedport)
                # Store the assigned ID in the nodeinfo.json file or process it as needed
                save_client_id(assigned_id,assignedport)
                return ([assigned_id,assignedport])

        except Exception as e:
            print(f"Error during request for ID: {e}")

        finally:
            client.close()

        retries += 1
        print(f"Retrying request for ID in {RETRY_INTERVAL} seconds (Attempt {retries}/{MAX_RETRIES})")
        time.sleep(RETRY_INTERVAL)

    print("Node shutting down. Unable to obtain ID.")
    os._exit(1)

def send_verification_request(client_socket, client_id,address):
    retries = 0
    while retries <= MAX_RETRIES:
        # Send a verification request to the server
        client_socket.send((json.dumps(["N002",client_id,address])).encode('utf-8'))

        # Receive the server's response with the confirmation code and verification result
        temp=(client_socket.recv(1024)).decode('utf-8')
        response = json.loads(temp)
        confirmation_code = response[0]

        if confirmation_code == "A002":
            print(f"ID verified")
            return

        retries += 1
        print(f"Retrying verification in {RETRY_INTERVAL} seconds (Attempt {retries}/{MAX_RETRIES})")
        time.sleep(RETRY_INTERVAL)

def heartbeat(client_socket):

    while True:
        # Continue with sending pings or other actions as needed
        client_socket.send((json.dumps(check_folder_size(filepath))).encode('utf-8'))
        time.sleep(1)

# ... (other functions remain the same)

if __name__ == "__main__":
    ipaddress=([l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])
    server_address = ("localhost", 50000)  # Change to the server's address and port
    client_id=None
    port=None
    # Check if nodeinfo.json file exists
    client_info = load_client_id()
    if client_info is None:
        # If the file doesn't exist, request a new client ID
        result = send_request_for_id(server_address)
        client_id=result[0]
        port=result[1]
    else:
        client_id=client_info["nodeid"]
        port=client_info["nodeport"]
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(server_address)

    try:
        send_verification_request(client_socket, client_id, [ipaddress, port])

        # Start FTP server and heartbeat threads concurrently
        blockreceiver_thread = threading.Thread(target=blockreceiver, args=(ipaddress, port))
        blockreceiver_thread.start()

        heartbeat_thread = threading.Thread(target=heartbeat, args=(client_socket,))
        heartbeat_thread.start()

        # Wait for both threads to finish
        blockreceiver_thread.join()
        heartbeat_thread.join()

    finally:
        client_socket.close()
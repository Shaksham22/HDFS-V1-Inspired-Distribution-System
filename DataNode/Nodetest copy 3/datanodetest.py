import socket
import time
import os
import json
import warnings
import threading
from nodespacevirtualizer import check_folder_size
import csv

client_id=None
filepath=os.path.dirname(os.path.abspath(__file__))
blockdata=[]

MAX_RETRIES = 3
RETRY_INTERVAL = 300  # 5 minutes in seconds

import json
import hashlib
import os

def map_function(csvdata):
    delay_map = {}
    
    # Assuming the first row is the header, so we skip it
    header = csvdata[0]
    data_rows = csvdata[1:]

    for row in data_rows:
        carrier = row[1]
        delay = float(row[7]) if row[7] != '' else 0

        if delay > 0:
            if carrier not in delay_map:
                delay_map[carrier] = {'delay_sum': 0, 'count': 0}

            delay_map[carrier]['delay_sum'] += delay
            delay_map[carrier]['count'] += 1

    for carrier, stats in delay_map.items():
        yield (carrier, stats)

def reduce_function(mapped_data):
    count_map = {}
    delay_map = {}

    for carrier, stats in mapped_data:
        if carrier not in count_map:
            count_map[carrier] = 0
            delay_map[carrier] = 0.0

        count_map[carrier] += stats['count']
        delay_map[carrier] += stats['delay_sum']

    result = [(carrier, {'count': count_map[carrier], 'delay_sum': delay_map[carrier], 'avg_delay': delay_map[carrier] / count_map[carrier] if count_map[carrier] > 0 else 0}) for carrier in count_map]

    return result

def checksum(data):
    if isinstance(data, str):
        # If the input is a string, encode it to bytes
        data = data.encode('utf-8')
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()

def handle_metadata(code, info):
    with open(f"blockmetadata.json", 'r') as file:
        file=file.read()
        metadatadict=json.loads(file)
    if code == 1:
        # Storing metadata
        blockid, length, state, hashvalue = info
        if(blockid not in metadatadict):
            metadatadict[blockid] = {
                "length": length,
                "state": state,
                "checksum": hashvalue
            }
        else:
            return(False)

        json_data = json.dumps(metadatadict)

        # Write the JSON data to the specified output file
        try:
            with open(f"blockmetadata.json", 'w') as file:
                file.write(json.dumps(metadatadict))
                return True
        except:
            return False

    elif code == 2:
        # Retrieving metadata
        blockid = info[0]
        if(blockid in metadatadict):
            return (metadatadict[blockid])
        else:
            return False

    elif code == 3:
        # Verifying checksum
        blockid, checksum_value = info
        if(blockid in metadatadict):
            if(metadatadict[blockid]["checksum"] is not checksum_value):
                metadatadict[blockid]["state"]="corrupt"
                return(True)

    elif code == 4:
        # Making changes to metadata
        blockid, changes = info
        for key, value in changes.items():
            metadatadict[key] = value

        json_data = json.dumps(metadatadict, indent=2)

        # Write the updated JSON data back to the file
        try:
            with open("metadatafile.json", 'w') as file:
                file.write(json_data)
            return True
        except:
            return False

def blockreport(client_socket):
        with open(f"blockmetadata.json", 'w') as file:
            d={}
            file.write(json.dumps(d))
        while True:
            with open(f"blockmetadata.json", 'r') as file:
                file=file.read()
                metadatadict=json.loads(file)
        # Continue with sending pings or other actions as needed
                client_socket.send((json.dumps(["mes1",metadatadict])).encode('utf-8'))
                time.sleep(3600)

def transmitfile(blockname,partitionnumber, server_address, server_port,partitiondata,targetnodes,filename):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((server_address, server_port))
    print("sending")
    partitioninfo=[blockname,partitionnumber,partitiondata,targetnodes,filename]
    client.sendall((json.dumps(partitioninfo)).encode('utf-8'))
    # confirmation_message = client.recv(1024)
    # if(confirmation_message==b"NC001"):
    client.close()
    print(blockname+" sent successfully.")
    # else:
    #     print("Error here")

def blockreceiver(server_address, server_port,namenode_address):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((server_address, server_port))
    server.listen(20)  # Listen for incoming connections

    print(f"Blockreceiver listening on {server_address}:{server_port}")

    while True:
        client_socket, client_address = server.accept()
        print(f"Connection established with {client_address}")
        blockinfo = b""
        while True:
            data = client_socket.recv(128*1000*1000)
            if not data:
                break
            blockinfo += data

        if not blockinfo:
            break

        blockinfo = json.loads(blockinfo.decode('utf-8'))
        print("File received")
        #blockinfo[0] is block name (i.e. block id assigned by the name node)
        #blockinfo[1] is block(partition) number w.r.t file (i.e. first block of the file or second block of the file)
        #blockinfo[2] is block data to be stored
        #blockinfo[3] is the target nodes
        #blockinfo[4] is file name

        with open((filepath + "/blocklist/" + blockinfo[0]+".csv"), 'w') as file:
            file.write(blockinfo[2])
        print("file written")
        # client_socket.sendall(b"NC001")
        print("File received successfully.")
        namenode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        namenode_socket.connect(namenode_address)
        namenode_socket.send((json.dumps(["N003",blockinfo[4],blockinfo[0],blockinfo[1],client_id])).encode('utf-8'))
        print("Acknowlegdement of "+blockinfo[0]+" sent to namenode.")
        namenode_socket.close()



        if len(blockinfo[3]) != 0:
            address = blockinfo[3].pop(0)
            print(blockinfo[0], blockinfo[1], blockinfo[3])
            transmitfile(blockinfo[0], blockinfo[1], address[1][0], address[1][1], blockinfo[2], blockinfo[3],blockinfo[4])
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
        
def nextfile(filename,namenode_address):
        namenode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        namenode_socket.connect(namenode_address)
        print(filename)
        namenode_socket.send((json.dumps(["N004",filename])).encode('utf-8'))
        print("sent")
        message=(namenode_socket.recv(1024*1000)).decode('utf-8')
        namenode_socket.close()
        if(message[2]==client_id):
            with open(filepath + "/blocklist/" +message[1]+".csv", 'rt', newline='') as fileb:
                csv_readerb = csv.reader(fileb)
                remotefile=next(csv_readerb, None)
##                    print(remotefile)
                if("*^" not in remotefile[0]):
                    print("Error here",file)
                remotefile[0]=remotefile[0].replace("*^","")
##                    print(4.5,remotefile)
                return(remotefile)
        else:
            othernode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print(message)
            addre=message[3]
            print(addre)
            othernode_socket.connect(addre)
            othernode_socket.send((json.dumps(["N004",message[1]])).encode('utf-8'))
            rcvremotefile=(client_socket.recv(1024*1000)).decode('utf-8')
            if("*^" not in rcvremotefile[0]):
                print("Error here",file)
            rcvremotefile[0]=rcvremotefile[0].replace("*^","")
##            print(4.5,remotefile)
            return(rcvremotefile)
        
            
            
    


def requesthandler(server_address, server_port,namenode_address):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((server_address, server_port))
    server.listen(20)  # Listen for incoming connections
    print(f"RequestHandler listening on {server_address}:{server_port}")
    while True:
        client_socket, client_address = server.accept()
        print("Accepted client connection from", client_address)
        message = json.loads((client_socket.recv(1024)).decode('utf-8'))
        if(message[0]=="NC002"):
            print(message)
            with open((filepath + "/blocklist/" +message[1]+".csv"), 'r') as file:
                csv_reader = csv.reader(file)
                csvdata = list(csv_reader)
                if("*^" in csvdata[0][0]):
                    print("1",csvdata[0][0])
                    csvdata.pop(0)
                if("^#" in csvdata[-1][-1]):
                    csvdata.pop(-1)
                #     print(2,namenode_address)
                #     remotefiledata=nextfile(message[1],namenode_address)
                #     temp=remotefiledata.pop(0)
                #     csvdata[-1][-1]=csvdata[-1][-1].replace("^#",temp)
                #     print(6,csvdata[-1][-1])
                #     csvdata[-1]=csvdata[-1]+remotefiledata
                # print(csvdata[0])
                # print(csvdata[-1])
                # client_socket.close()
        elif(message[0]=="NC020"):
            path=filepath+"/blocklist/"
            filelist = [file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))]
            try:
                filelist.remove(".DS_Store")
            except:
                pass
            data=[]
            print("Applying Map-Reduce to the following folders present in this node (The process will take some time):-")
            for i in filelist:
                print(i)
                with open((filepath + "/blocklist/" +i), 'r') as file:
                    csv_reader = csv.reader(file)
                    csvdata = list(csv_reader)
                    if("*^" in csvdata[0][0]):
                        # print("1",csvdata[0][0])
                        csvdata.pop(0)
                    if("^#" in csvdata[-1][-1]):
                        csvdata.pop(-1)
                    data=data+csvdata
            mapped_data = map_function(data)
            reduced_result = reduce_function(mapped_data)   
            print("Count Map:")
            for carrier, stats in reduced_result:
                print(f"{carrier}: {stats['count']} flights")

            print("\nAverage Delay Map:")
            for carrier, stats in reduced_result:
                print(f"{carrier}: {stats['avg_delay']} average delay minutes")


        elif(message[0]=="NC003"):
            with open(filepath + "/blocklist/" +message[1]+".csv", 'rt', newline='') as fileb:
                csv_readerb = csv.reader(fileb)
                remotefile=next(csv_readerb, None)
                response=["NC003",remotefile]
                client_socket.send((json.dumps(response)).encode('utf-8'))
                client_socket.close()

        


def heartbeat(client_socket):
    while True:
        # Continue with sending pings or other actions as needed
        client_socket.send((json.dumps(["mes0",check_folder_size(filepath)])).encode('utf-8'))
        time.sleep(3)

# ... (other functions remain the same)

if __name__ == "__main__":
    ipaddress=([l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])
    server_address = (ipaddress, 50000)  # Change to the server's address and port
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
        blockreceiver_thread = threading.Thread(target=blockreceiver, args=(ipaddress, port,server_address))
        blockreceiver_thread.start()

        heartbeat_thread = threading.Thread(target=heartbeat, args=(client_socket,))
        heartbeat_thread.start()

        blockreport_thread = threading.Thread(target=blockreport, args=(client_socket,))
        blockreport_thread.start()

        requesthandler_thread = threading.Thread(target=requesthandler, args=(ipaddress,port-20000,server_address,))
        requesthandler_thread.start()

        # Wait for both threads to finish
        blockreceiver_thread.join()
        heartbeat_thread.join()
        blockreport_thread.join()

    finally:
        client_socket.close()
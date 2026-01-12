
import socket
import time
from prettytable import PrettyTable
import threading
import json
import random

nodes = {}
id_lock = threading.Lock()  # Lock for thread-safe ID assignment
printtableduration=30
replicationfactor=3
nodeportid=50001
clientportid=40001
nodeid = 0
blockid = 0
blocknodeinfo={}
blockfileinfo={}

def portassign(code):
    global nodeportid,clientportid
    if(code=="N001"):
        port=nodeportid
        nodeportid+=1
        return(port)
    elif(code=="C001"):
        port=clientportid
        clientportid+=1
        return(port)
    

def idassign(code):
    global nodeid,blockid
    with id_lock:
        if(code=="N001"):
            nodeid+=1
            return str(nodeid).zfill(4)# Ensure the ID is a 4-digit integer string with leading zeros
        elif(code=="C001"):
            blockid+=1
            return str(blockid).zfill(8)  # Ensure the ID is a 8-digit integer string with leading zeros

# def analyseblockreport():

# def acceptuserconnection():
#     while True:
#         client_socket, client_address = server.accept()
#         client_message = json.loads((client_socket.recv(1024)).decode('utf-8'))
#         print("Accepted client connection from", client_address)
#         if  (client_message[0] == "C001"):
#             # client_id = idassign()
#             # print(f"Assigned ID {client_id} to client at {client_address}")
#             distributenodelist=distribution(client_message[1],nodes,replicationfactor)
#             confirmation_code = "A003"
#             response = [confirmation_code, distributenodelist]
#             client_socket.send((json.dumps(response)).encode('utf-8'))
#             print("Node Assigned")
#             # Close the original client_socket in the main thread
#             client_socket.close()  
        

            # Start a new thread to handle the client

        # elif client_message[0] == "C002":
        #     client_id = client_message[1]
        #     if client_id in clients:
        #         clients[client_id] = [time.time(), "Online"]
        #         print(f"Client {client_id} verified")
        #     else:
        #         print(f"Client not verified")
        #         continue
        #     confirmation_code = "A004"
        #     response = [confirmation_code]
        #     client_socket.send((json.dumps(response)).encode('utf-8'))
        #     # Close the original client_socket in the main thread
        #     handle_datanode_thread = threading.Thread(target=handle_datanode, args=(client_socket, client_id))
        #   handle_datanode_thread.start()

        # else:
        #     print(f"Unknown status code received from client")


def print_node_status_table():
    while True:
        time.sleep(printtableduration)
        for node_id in nodes:
            if(len(nodes[node_id])==0):
                continue
            if time.time() - nodes[node_id][0] > 30:
                nodes[node_id][1] = "Paused"
            if time.time() - nodes[node_id][0] > 60:
                nodes[node_id][1] = "Offline"

        table = PrettyTable(["node ID", "Status"])
        for node_id in nodes:
            if(len(nodes[node_id])==0):
                continue
            table.add_row([node_id, nodes[node_id][1]])
        print("\nnode Status Table:")
        print(table)
        print(blocknodeinfo)
        print(blockfileinfo)

def handle_datanode(node_socket, node_id,node_address):
    while True:
        data = json.loads((node_socket.recv(1024)).decode('utf-8'))
        if not data:
            break
        if(data[0]=="mes0"):
            nodes[node_id] = [time.time(),"Online",data[1],node_address]
        elif(data[0]=="mes1"):
            print(len(data))


def node_server():
    ipaddress=([l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])
    node_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_server_socket.bind((ipaddress, 50000))  # Adjust the port as needed
    node_server_socket.listen(5)
    print("Node Server listening on",ipaddress,":50000\n")

    while True:
        node_socket, node_address  = node_server_socket.accept()
        node_message = json.loads((node_socket.recv(1024)).decode('utf-8'))
        if node_message[0] == "N001":
            node_id = idassign(node_message[0])
            print(f"Assigned ID {node_id} to node at {node_address}")
            nodes[node_id]=[]
            confirmation_code = "A001"
            port=portassign(node_message[0])
            response = [confirmation_code, node_id,port]
            node_socket.send((json.dumps(response)).encode('utf-8'))
            # Close the original node_socket in the main thread
            node_socket.close()

            # Start a new thread to handle the node

        elif node_message[0] == "N002":
            node_id = node_message[1]
            print(node_id,node_message[2])
            if node_id in nodes:
                nodes[node_id] = [time.time(), "Online"]
                print(f"node {node_id} verified")
            else:
                print(f"node not verified")
                continue
            confirmation_code = "A002"
            response = [confirmation_code]
            node_socket.send((json.dumps(response)).encode('utf-8'))
            # Close the original node_socket in the main thread
            node_thread = threading.Thread(target=handle_datanode, args=(node_socket, node_id,node_message[2]))
            node_thread.start()
            
        elif node_message[0] == "N003":
            #node_message[1] is file name
            #node_message[2] is block name (i.e. block id assigned by the name node)
            #node_message[3] is block(partition) number w.r.t file (i.e. first block of the file or second block of the file)
            #node_message[4] is nodeid

            if(node_message[1] not in blockfileinfo):
                blockfileinfo[node_message[1]]={}
            if(node_message[3] not in blockfileinfo[node_message[1]]):
                blockfileinfo[node_message[1]][node_message[3]]=node_message[2]
            if(node_message[2] not in blocknodeinfo):
                blocknodeinfo[node_message[2]]=[node_message[1],node_message[3],[]]
            blocknodeinfo[node_message[2]][2].append(node_message[4])
        elif node_message[0] == "N004":
            print("Remoteread accessed")
            #For remote read
            filename=blocknodeinfo[node_message[1]][0]
            partitionnumber=blocknodeinfo[node_message[1]][1]+1
            nextblockname=blockfileinfo[filename][partitionnumber]
            nextblocknode=random.choice(blocknodeinfo[nextblockname][2])
            nextblocklocation=nodes[nextblocknode][3]
            nextblocklocation[1]=nextblocklocation[1]-20000
            print(nextblocklocation)
            response=["A0010",nextblockname,nextblocknode,nextblocklocation]
            print(response)
            node_socket.send((json.dumps(response)).encode('utf-8'))
            node_socket.close()
        else:
            print(f"Unknown status code received from node at {node_address}")
       

def user_server():
##    try:
    user_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    user_server_socket.bind(('localhost', 40000))  # Adjust the port as needed
    user_server_socket.listen(100)
    print("User Server listening on 0.0.0.0:40000\n")

    while True:
        user_socket, user_address = user_server_socket.accept()
        client_message = json.loads((user_socket.recv(1024)).decode('utf-8'))
        print("Accepted client connection from", user_address)
        from distributionlogic import distribution
        global blockfileinfo
        global blocknodeinfo
        if client_message[0] == "C001":
            block_id=idassign(client_message[0])
            distributenodelist = distribution(client_message[1], nodes, replicationfactor)
            response = ["A003", block_id ,distributenodelist[1]]
            if distributenodelist[0] != "DL004":
                print(distributenodelist[1])
            else:
                print("Nodes Assigned")
                response[0] = "A004"
            user_socket.send((json.dumps(response)).encode('utf-8'))
            # Close the original user_socket in the main thread
            user_socket.close()
        elif client_message[0] == "C002":
            response = ["A005", blockfileinfo]
            user_socket.send((json.dumps(response)).encode('utf-8'))
            user_socket.close()
        elif client_message[0] == "C003":
            blocknodeinfo={}
            blockfileinfo={}
            response = ["A006"]
            user_socket.send((json.dumps(response)).encode('utf-8'))
            user_socket.close()
        elif client_message[0] == "C004":
            targetblock=client_message[1]
            targetnode=random.choice(blocknodeinfo[targetblock][2])
            print(targetnode,nodes[targetnode])
            response = ["A007",targetnode,nodes[targetnode][3]]
            user_socket.send((json.dumps(response)).encode('utf-8'))
            user_socket.close()
        elif client_message[0] == "C005":
            response = ["A008",nodes]
            user_socket.send((json.dumps(response)).encode('utf-8'))
            user_socket.close()
        else:
            print(f"Unknown status code received from client")
##    except Exception as e:
##        print(e)
##        time.sleep(15)
##        if 'user_socket' in locals() and user_socket:
##            user_socket.close()
##        user_server_socket.close()
        

if __name__ == "__main__":
    user_server_thread = threading.Thread(target=user_server, args=())
    node_server_thread = threading.Thread(target=node_server, args=())
    nodestatustablethread = threading.Thread(target=print_node_status_table, args=())
    user_server_thread.start()
    node_server_thread.start()
    nodestatustablethread.start()
    # Add any additional code that needs to run in the main thread
    # For example, code that interacts with both user and node connections

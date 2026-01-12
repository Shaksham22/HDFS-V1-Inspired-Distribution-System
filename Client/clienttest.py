import socket
import json
from splitter import splitter
import os
from ftplib import FTP
import time
fileinfo={}
from prettytable import PrettyTable
path=os.path.dirname(os.path.abspath(__file__))+"/partitions/"
filelist = [file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))]
try:
    filelist.remove(".DS_Store")
except:
    pass

def transmitfile(blockname,partitionnumber, server_address, server_port,partitiondata,targetnodes,filename):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((server_address, server_port))
    partitioninfo=[blockname,partitionnumber,partitiondata,targetnodes,filename]
    client.sendall((json.dumps(partitioninfo)).encode('utf-8'))
    # confirmation_message = client.recv(1024)
    # if(confirmation_message==b"NC001"):
    client.close()
    print(blockname+" sent successfully.")
    # else:
    #     print("Error here")

# Example usage
        
def sendrequest(message):
    # Set up the client socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Connect to the server
    server_address = ("localhost",40000)  # Change the port number here
    client_socket.connect(server_address)
    # Send data to the server
    client_socket.send((json.dumps(message)).encode('utf-8'))
    if(message[0]=="C001"):
        print("File request sent")
        targetnodelist=json.loads((client_socket.recv(1024)).decode('utf-8'))
        if(targetnodelist[0]=="A003"):
            print(targetnodelist[1])
        elif(targetnodelist[0]=="A004"):
            print("Following are the nodes assigned to store the block: ")
            for nodes in (targetnodelist[2]):
                print(nodes[0])
            return(targetnodelist[1],targetnodelist[2])
        else:
            print("Some Error")
    elif(message[0]=="C002"):
        print("Accessing the data from the NameNode")
        resultfile=json.loads((client_socket.recv(1024*1000)).decode('utf-8'))
        return(resultfile)
    elif(message[0]=="C004"):
        print("Accessing the NameNode to know the data node for ",message[1])
        print(message)
        resultfile=json.loads((client_socket.recv(1024*1000)).decode('utf-8'))
        return(resultfile)
    elif(message[0]=="C005"):
        print("Accessing the NameNode to know the data nodes")
        print(message)
        resultfile=json.loads((client_socket.recv(1024*1000)).decode('utf-8'))
        return(resultfile)
    client_socket.close()

def inputfile():
    # ipaddress=([l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])
    filename,result=splitter()
    print(filename)
    print(len(result))
    # input("Press enter to continue")
    # print(result[0],len(result[1]))
    for i,a in enumerate(result):
        print("\n",i)
        filesize = len(a)
        message=["C001",filesize]
        nodeid,targetnodes=sendrequest(message)
        address=targetnodes.pop(0)
        blockname=nodeid
        transmitfile(blockname,i,address[1][0], address[1][1],a,targetnodes,filename)
        time.sleep(1)
        # input("Press enter to continue")

def printfiletable():
    message=["C002"]
    results=sendrequest(message)
    print("Following files are present in the system:")
    table = PrettyTable(["S.No", "File Name"])
    c=1
    for i in results[1]:
        table.add_row([c, i])
        c+=1
    print(table)
def printblocktable():
    message=["C002"]
    results=sendrequest(message)
    print("Following files are present in the system:")
    table = PrettyTable(["S.No", "File Name","Partition Number","Block Id"])
    c=1
    for i in results[1]:
        for j in results[1][i]:
            table.add_row([c, i,j,results[1][i][j]])
            c+=1
    print(table)

def printresult():
    return(2)


def clearall():
    message=["C003"]
    results=sendrequest(message)
    if(results[0]=="A006"):
        print("All info cleared")
    else:
        print("Some error occured. Please try again")

def printblock(blockid):
    message=["C004",blockid]
    print(message)
    result=sendrequest(message)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((result[2][0], result[2][1]-20000))
    response=["NC002",blockid,result[1]]
    print(response)
    client.send((json.dumps(response)).encode('utf-8'))
    client.close()

def printall():
    message=["C005"]
    print(message)
    result=sendrequest(message)
    print(result)
    for i in result[1]:
        print(i,result[1][i][-1])
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sadd=result[1][i][-1][0]
        padd=int(result[1][i][-1][1])-20000
        print((sadd,padd))
        client.connect((sadd,padd))
        response=["NC020"]
        print(response)
        client.send((json.dumps(response)).encode('utf-8'))
        client.close()

while(True):
    # try:
    query=input("Enter a query> ")
    res=query.split(" ")
    if(res[0].lower()=="insert"):
        inputfile()
    elif(res[0].lower()=="print"):
        if(len(res)==1):
            print("Try again")
            continue
        if(res[1].lower()=="filetable"):
            print("Printing File Table")
            printfiletable()
        elif(res[1].lower()=="blocktable"):
            print("Printing Block Table")
            printblocktable()
        elif(res[1].lower()=="block"):
            print("Printing block")
            if(len(res)<2):
                print("Wrong input try again")
                continue
            file=printblock(res[2])

        elif(res[1].lower()=="on"):
            if(len(res)<2):
                print("Wrong input try again")
                continue
            print("Printing data on nodes")
            printall()

        else:
            print("Please try again!")
    elif(res[0].lower()=="exit"):
        break
    elif(res[0].lower()=="clear"):
        clearall()
    else:
        print("Invalid Input")
    # except:
    #     print("Error occured")
    #     continue
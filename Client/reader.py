import os
import json
import time
testlist=[]

path=os.path.dirname(os.path.abspath(__file__))+"/partitions/"
filelist = [file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))]
print(filelist)
samplefilename=filelist[0]
print(samplefilename)
samplefilename=samplefilename.split(".")
samplefilename=''.join(samplefilename[:-1])
samplefilename=samplefilename[:-1]

try:
    filelist.remove(".DS_Store")
except:
    pass
filenumbers=len(filelist)
input("Press Enter to Continue")



def logicalsplitter(s,fileno):
    print("primaryfilename",samplefilename,fileno)
    logicallist=[]
    if(fileno==1):
        ##add more condition here to handle the binding list or dictionary. a binding list is a list/dictionary that holds all the maintest lists or dictionary
        if(s[0]=="["):
            s=s[1:]
    if(fileno==filenumbers):
        if(s[-1]=="]"):
            s=s[:-1]
    stack=[]
    starter=0
    for i,a in enumerate(s):
        if(a=="{"):
            stack.append("{")
        elif(a=="}"):
            stack.pop()
            if(len(stack)==0):
                testlist.append(s[starter:i+1])
##                print(s[starter:i+1])
                logicallist.append(s[starter:i+1])
                starter=i+1

    filelist.remove(samplefilename+str(fileno)+".json")
    if(len(stack)==0):
        return ([logicallist,""])
##Theres a problem with returning this as it is more than one dictionary, so it needs to be returned as list of dictionary.
## Everytime the stack is empty, pop out the dictionary as it has been validated.
    else:
        if(s[-2:]!="^#"):
            print("Error has occured while validating the file split")
            print(testlist[-1])
            exit()
        s=s[starter:]
        s=s.replace("^#","")
        j=fileno+1
        while (True):
            print("remotereadfile",j)
            readresult=remoteread(samplefilename+str(j)+".json",stack)
            s=s+readresult[1]
            if(readresult[0]==False):
                j+=1
            else:
                s=s.replace("*^","")
                testlist.append(s)
                print(s)
                logicallist.append(s)
                return([logicallist,readresult[1]])






def remoteread(filename,stack):
    with open(path+filename,"r") as remotefile:
        remoteportion=remotefile.read()
    if(remoteportion[:2]!="*^"):
        print("Error at validating Remoteread")
        print(testlist[-1])
        exit()
    for i,a in enumerate(remoteportion):
        if(a=="{"):
            stack.append("{")
        elif(a=="}"):
            stack.pop()
            if(len(stack)==0):
                return(True,remoteportion[:i+1])

    if(len(stack)!=0):
        if(remoteportion[-2:]!="^#"):
            print("Error at validating Remoteread continuation")
            print(testlist[-1])
            exit()
        filelist.remove(filename)
        return(False,remoteportion.replace("^#",""))


def cleaner(data):
    if(type(data)==list):
        for i,a in enumerate(data):
            if(a[0]!="{"):
                temp=a.find("{")
                temp=a[temp:]    
                data[i]=json.loads(temp)
            else:
                data[i]=json.loads(a)
                
            
    if(type(data)==dict):
        for i in (data):
            for j,a in enumerate(data[i]):
                if(a[0]!="{"):
                    temp=a.find("{")
                    temp=a[temp:]    
                    data[i][j]=json.loads(temp)
                else:
                    data[i][j]=json.loads(a)
            return(data)


logicalsplits={}
logicalsplitid=1
remotereadresult=None
for i in range(1,filenumbers+1):
    if(samplefilename+str(i)+".json" not in filelist):
        continue
    print(i)
    with open(path+samplefilename+str(i)+".json","r") as file:
        split=file.read()
    if(remotereadresult!=None):
        split=split.replace(remotereadresult,"")
        remotereadresult=None
    splitresult=logicalsplitter(split,i)
    logicalsplits[logicalsplitid]=splitresult[0]
    if(len(splitresult[1])!=0):
        remotereadresult=splitresult[1]
##        print(splitresult[1],"\n")
##        print(splitresult[0])
    logicalsplitid+=1
    

##print(logicalsplits)
    
testlist=cleaner(testlist)
logicalsplits=cleaner(logicalsplits)













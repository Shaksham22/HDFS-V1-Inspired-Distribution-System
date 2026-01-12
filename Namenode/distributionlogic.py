blocksize=1024*1024*64
def distribution(blockfilesize,nodes,replicationfactor):
    if(len(nodes)==0):
        return(["DL001","No nodes available"])
    nodestrue={}
    nodesfalse={}
    targetnodelist=[]
    for i in nodes:
        if(nodes[i][1]=="Online"):
            if(nodes[i][2][2]==True):
                nodestrue[i]=nodes[i]
            else:
                percentage=nodes[i][2][1]
                maxsize=nodes[i][2][0]
                presentspace=maxsize*(percentage/100)
                if(presentspace>blockfilesize):
                    nodesfalse[i]=nodes[i]
    if(len(nodestrue)+len(nodesfalse)<replicationfactor):
        return("DL002","Not enough nodes available")
    nodestrue=dict(sorted(nodestrue.items(), key=lambda x: x[1][2][1],))
    nodesfalse=dict(sorted(nodesfalse.items(), key=lambda x: x[1][2][1], reverse=True))
    for i in nodesfalse:
        targetnodelist.append([i,nodes[i][3]])
        if(len(targetnodelist)>=replicationfactor):
            break
    if(len(targetnodelist)<replicationfactor):
        for i in nodestrue:
            targetnodelist.append([i,nodes[i][3]])
            if(len(targetnodelist)>=replicationfactor):
                break
    if(len(targetnodelist)<replicationfactor):
        return("DL003","Not enough nodes available")
    return(["DL004",targetnodelist])

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
import os
import time
import subprocess
import json
import ast
from collections import OrderedDict
from insert_data import select_all_keys

def put_watch(server_name):
    print("--- Putting watch on server "+server_name+" ---")
    zk = KazooClient("localhost:2181")
    zk.start()
    children = zk.get("/big_data_assign/"+server_name,watch=watch_host)
	    
def watch_host(event):
	print("change!!")
	print(event)
	if(event.type=="DELETED"):
	    print("do something")
	    modify_mappings()
	if(event.type=="CHANGED"):
	    return -1

def modify_mappings():
    zk = KazooClient("localhost:2181")
    zk.start()
    children = zk.get_children("/big_data_assign")
    for each in children:
        path="/big_data_assign/"+each
        print(path)
        c = zk.get(path)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        #print("here4",c,type(c))
        #for key, value in c.items():
        key=list(c.items())[0]
        key=key[0]
        print(key)
        d={}
        d[key]=select_all_keys(each)
        d=OrderedDict(d)
        modify_server(d,each)
        #print(d)
    for each in children:
        path="/big_data_assign/"+each
        print(path)
        c = zk.get(path)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        #print("here4",c,type(c))
        #for key, value in c.items():
        key=list(c.items())[0]
        key=key[0]
        print(key)
        d={}
        d[key]=select_all_keys(each)
        d=OrderedDict(d)
        register_server_in_others(d)        


def modify_server(port_key_mapping,server_name):
	print("Registering server with zookeeper... ")
	zk = KazooClient("localhost:2181")
	zk.start()
	zk.add_listener(my_listener)
	zk.ensure_path("/big_data_assign")
	port_key_mapping=str(port_key_mapping)
	#print("port num: ",port_num,type(port_num))
	port_key_mapping=port_key_mapping.encode()
	# Registering program ID in ephemeral node in zookeeper
	zk.get_children("/big_data_assign/")
	path="/big_data_assign/"+server_name
	zk.set(path,port_key_mapping)
	return 1
	
        
def my_listener(state):
    if state == KazooState.LOST:
    # Register somewhere that the session was lost
        print("Lost")
    elif state == KazooState.SUSPENDED:
    # Handle being disconnected from Zookeeper
        print("Suspended")
    else:
    # Handle being connected/reconnected to Zookeeper
    	print("Being Connected/Reconnected")

#port_key_mappings - {port:[list of keys]}
def register_server(port_key_mapping,server_name):
	print("Registering server with zookeeper... ")
	zk = KazooClient("localhost:2181")
	zk.start()
	zk.add_listener(my_listener)
	zk.ensure_path("/big_data_assign")
	port_key_mapping=str(port_key_mapping)
	#print("port num: ",port_num,type(port_num))
	port_key_mapping=port_key_mapping.encode()
	# Registering program ID in ephemeral node in zookeeper
	zk.get_children("/big_data_assign/")
	path="/big_data_assign/"+server_name
	zk.create(path,value=bytes(port_key_mapping),acl=None,ephemeral=True,makepath=True)
	return 1

def check_active(port):
    zk=KazooClient("localhost:2181")
    zk.start()
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    path="/big_data_assign/"
    for each in children:
        path_child=path+each
        #print(path_child)
        c = zk.get(path_child)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        #print("here4",c,type(c))
        #for key, value in c.items():
        key=list(c.items())[0]
        key=key[0]
        #print(key,type(key))
        if(key==port):
            active=True
            break;
        else:
            active=False
    #print(active)
    return active
                
    
def register_server_in_others(port_key_mapping):
    #print(port_key_mapping)
    for key_to_be_added in port_key_mapping:
        print(key_to_be_added)
    value_to_be_added=port_key_mapping[key_to_be_added]
    print("Registering server with others... ")
    zk=KazooClient("localhost:2181")
    zk.start()
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    path="/big_data_assign/"
    #print("children",children)
    for each in children:
        path_child=path+each
        print(path_child)
        c = zk.get(path_child)
        #print("here1",c,type(c))
        c=c[0]
        #print("here2",c,type(c))
        c=c.decode()
        #print("here3",c,type(c))
        #c=ast.literal_eval(c)
        c = eval(c, {'OrderedDict': OrderedDict})
        #print("here4",c,type(c))
        for key, value in c.items():
            #print(key,value)
            if(key==key_to_be_added):
                required=key
    children = zk.get_children("/big_data_assign")
    #print(required,type(required))        
    #print(children,type(children))
    others=[]
    for each in children:
        if(each!=required):
            others.append(each)
    #print(others)
    path="/big_data_assign/"
    for each in others:
        path_child=path+each
        #print(path_child)
        children = zk.get(path_child)
        children=children[0]
        children=children.decode()
        #children=ast.literal_eval(children)
        children = eval(children, {'OrderedDict': OrderedDict})
        children[key_to_be_added]=value_to_be_added
        children=str(children)
        children=children.encode()
        zk.set(path_child,children)
        #print(children)
        

def print_children():
	zk = KazooClient("localhost:2181")
	zk.start()
	zk.add_listener(my_listener)
	zk.ensure_path("/big_data_assign")
	#print("All children")
	children = zk.get_children("/big_data_assign")
	print("children: ",children)
	return children

#modifying
def modify_add():
    zk = KazooClient("localhost:2181")
    zk.start()
    children=zk.get_children("/big_data_assign")
    for each in children:
        path="/big_data_assign/"+each
        c = zk.get(path)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        #print("here4",c,type(c))
        #for key, value in c.items():
        key=list(c.items())[0]
        key=key[0]
        print(key)
        d={}
        d[key]=select_all_keys(each)
        d=OrderedDict(d)
        d=str(d)
        zk.set(path,d)
    

def get_ports():
    zk = KazooClient("localhost:2181")
    zk.start()
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    available_ports=[]
    for each in children:
        seq="/big_data_assign/"+each
        avail_port=zk.get(seq)
        print(avail_port)
        avail_port=avail_port[0]
        avail_port=avail_port.decode()
        avail_port = eval(avail_port, {'OrderedDict': OrderedDict})
        #first element of Ordered Dict
        a=list(avail_port.items())[0]
        print("heree",a[0],type(a[0]))
        #avail_port=int(avail_port)
        available_ports.append(a[0])
        #print("avail port and type: ",avail_port,type(avail_port))
    return available_ports

        
def master_server():
    zk = KazooClient("localhost:2181")
    zk.start()
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    master=children[0]
    path="/big_data_assign/"+master
    print("master server path",path)
    return path

def server_exists(server_name):
    zk = KazooClient("localhost:2181")
    zk.start()
    children=zk.get_children("/big_data_assign")
    if(server_name not in children):
        return False
    else:
        return True
    

def register_children(path,keys_in_server,connected_port):
	print("Registering server with zookeeper... ")
	zk = KazooClient("localhost:2181")
	zk.start()
	zk.add_listener(my_listener)
	zk.ensure_path(path)
	#port_num=str(port_num)
	#print("port num: ",port_num,type(port_num))
	#port_num=port_num.encode()
	#keys_in_server=keys_in_server.encode()
	child_path=path+"/node"
	# Registering program ID in ephemeral node in zookeeper
	'''keys_in_server=json.dumps(keys_in_server).encode('utf-8')'''
	d=dict()
	d[connected_port]=keys_in_server
	d=str(d)
	#key_in_server=keys_in_server.encode("utf-8")
	zk.create(child_path,value=bytes(d,"utf-8"),acl=None,ephemeral=True,sequence=True,makepath=True)
	return 1	    


def get_server(entered_key):
    zk = KazooClient("localhost:2181")
    zk.start()
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    master_server=children[0]
    #print(master_server)
    master_info = zk.get("/big_data_assign/"+master_server)
    #print(master_info)
    #byte string
    key_mappings=master_info[0]
    #string
    key_mappings=key_mappings.decode()
    #Ordered Dict
    key_mappings = eval(key_mappings, {'OrderedDict': OrderedDict})
    for port,associated_keys in key_mappings.items():
        for each in associated_keys:
            if each==entered_key:
                required_port=port        
    #print(key_mappings)
    #print(required_port)
    return required_port



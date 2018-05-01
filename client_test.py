import socket 
import json
from random import randrange
from register_server import get_ports,get_server,check_active

host = "localhost" 
#port = get_port()
BUFFER_SIZE = 2000
option=1
a={}
available_ports = get_ports()
available_ports=available_ports[::-1]
print("here",available_ports)

def random_assign():
    #available_ports = get_ports()
    random_index = randrange(0,len(available_ports)-1)

    primary_server_id=random_index+1
    backup_server_id=((random_index+1)%3)+1
    print("PRIMARY SERVER ID:",primary_server_id)
    print("BACKUP SERVER ID:",backup_server_id)
    active_primary=check_active(available_ports[random_index])
    active_backup=check_active(available_ports[(random_index+1)%3])
    print("PRIMARY SERVER PORT:",available_ports[random_index])
    print("BACKUP SERVER PORT:",available_ports[(random_index+1)%3])
    return(active_primary,active_backup,available_ports[random_index],available_ports[(random_index+1)%3],primary_server_id,backup_server_id)

def put(key,value,port,replication):
    tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpClientA.connect((host, port))
    a=[]
    a.append(key)
    a.append(value)
    test=[]
    test.append(1)
    test.append(a)
    c=[]
    c.append(replication)
    test.append(c)
    b = json.dumps(test).encode('utf-8')
    tcpClientA.sendall(b)
    data = tcpClientA.recv(BUFFER_SIZE)
    print (" Client2 received data:", data.decode('utf-8'))
    tcpClientA.close()
    #modify_mappings(key,port)


while option!=3:
	print("1. Put (key value)")
	print("2. Get (key)")
	print("3. exit")
	option=int(input("Enter option"))

	if option==1:
		key=input("Enter key: ")
		value=int(input("Enter value: "))
		active_primary,active_backup,port_primary,port_backup,primary_server_id,backup_server_id=random_assign()
		print("active",active_primary,"active_backup",active_backup)
		if((active_primary==True)and(active_backup==True)):
		    put(key,value,port_primary,-1)
		    put(key,value,port_backup,-1)
		elif((active_primary==False)and(active_backup==True)):
		    #something
		    put(key,value,port_backup,primary_server_id)
		
	if option==2:
		MESSAGE=str(input("tcpClientA: Enter key:"))
		port=get_server(MESSAGE)
		print("Entering to port",port)
		tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		tcpClientA.connect((host, port))
		test=[]
		test.append(2)
		test.append(MESSAGE)
		b = json.dumps(test).encode('utf-8')
		tcpClientA.sendall(b)
		data = tcpClientA.recv(BUFFER_SIZE)
		print (" Client2 received data:", data.decode('utf-8'))
		tcpClientA.close()
	
	if option==3:
	    MESSAGE="Exiting!"
	    test=[]
	    test.append(3)
	    test.append(MESSAGE)
	    b = json.dumps(test).encode('utf-8')
	    tcpClientA.sendall(b)
	    tcpClientA.close()

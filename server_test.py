import socket 
from threading import Thread 
from socketserver import ThreadingMixIn 
import json
from insert_data import insert,print_all,select,select_all_keys,replicate
from register_server import register_server,print_children,master_server,register_children,register_server_in_others,put_watch,server_exists,modify_add,modify_mappings
import time
import collections

'''s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 9999))
s.listen(1)
conn, addr = s.accept()
b = b'''''
dictionary={}
# Multithreaded Python server : TCP Server Socket Thread Pool
class ClientThread(Thread): 
 
    def __init__(self,ip,port): 
        Thread.__init__(self) 
        self.ip = ip 
        self.port = port 
        print("[+] New server socket thread started for " + ip + ":" + str(port)) 
 
    def run(self): 
        
        while True : 
            b=b''
            tmp = conn.recv(4096) 
            print("Server received data:", tmp)
            b=b+tmp
            print("b is: ",b)
            #print(tmp)
            d=json.loads(b.decode('utf-8'))
            print("d is:",d)
            choice=int(d[0])
            #[1,['shreya',3],[to-replicate]]
            if choice==1:
            	key=d[1][0]
            	value=d[1][1]
            	replicate=d[2][0]
            	dictionary[key]=value
            	insert(key,value,"server1",replicate)
            	modify_mappings()
            	put_watch("server3")
            	#print("Dictionary on server side: ",dictionary)
            	MESSAGE="Value added!"
            	print_all("server1")
            	conn.send(MESSAGE.encode('utf-8'))
            #[2,'shreya']
            if choice==2:
            	client_key=d[1]
            	MESSAGE=str(select(client_key,"server1"))
            	conn.send(MESSAGE.encode('utf-8'))
            	
            #MESSAGE =input("Multithreaded Python server : Enter Response from Server/Enter exit:")
            '''if MESSAGE == 'exit':
                break'''
            
            if choice==3:
                MESSAGE="Exiting!"
                conn.send(MESSAGE.encode('utf-8'))
                tcpServer.close()
        
        #print("hello i am",d)
        #print(d[MESSAGE])

# Multithreaded Python server : TCP Server Socket Program Stub
TCP_IP = "localhost" 
TCP_PORT = 9999 
BUFFER_SIZE = 20  # Usually 1024, but we need quick response 
k=0
tcpServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
tcpServer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
#tcpServer.bind((TCP_IP, TCP_PORT)) 
tcpServer.bind(('', 0))
threads = [] 
connected_port=tcpServer.getsockname()[1]
print("Connected port: ",tcpServer.getsockname()[1])
#registering server
#register_server(connected_port)

    
    #master_path=master_server()
    #register_children(master_path,keys_in_server,connected_port)
    
def starting():
    check_master=print_children()
    if(check_master==[]):
        master=True
        print("I am the master server!")
        replicate("server1")
        keys_in_server=select_all_keys("server1") #list
        d={}
        d[connected_port]=keys_in_server
        d=collections.OrderedDict(d)
        register_server(d,"server1")
        time.sleep(10)
        #put_watch()
        register_server_in_others(d)
        
    else:
        print("I am the child server!")
        print("Registering with master!")
        replicate("server1")
        keys_in_server=select_all_keys("server1") #list
        d={}
        d[connected_port]=keys_in_server
        d=collections.OrderedDict(d)
        register_server(d,"server1")
        time.sleep(10)
        #put_watch()
        register_server_in_others(d)
    

while True:
    if(k==0):
        starting()
        k=k+1
    time.sleep(5)
    if(server_exists("server3")==True): 
        put_watch("server3") 
    print("Multithreaded Python server : Waiting for connections from TCP clients...") 
    #put_watch("server2")
    tcpServer.listen(4) 
    (conn, (ip,port)) = tcpServer.accept() 
    
    newthread = ClientThread(ip,port) 
    newthread.start() 
    threads.append(newthread) 
    #put_watch("server2")
    
    #put_watch()
 
for t in threads: 
    t.join()

import psycopg2

def insert(key,value,server_name,replication):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        #print('Database connection open')  
        query="""insert into """+server_name+"""(key,value,replication) values(%s,%s,%s)"""
        cur.execute(query,(key,value,replication))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    	    
def print_all(server_name):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        #print('Database connection open')  
        cur.execute("""select *from """+server_name)
        rows = cur.fetchall()
        for row in rows:
            print("Key: ",row[0])
            print("Value: ",row[1])
            print("----------")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')


def select(key,server_name):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        #print('Database connection open')  
        query="""select *from """+server_name+""" where key="""+"'"+key+"'"
        cur.execute(query)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        print(rows)
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')

def select_all_keys(server_name):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        #print('Database connection open')  
        query="""select key from """+server_name+""";"""
        cur.execute(query)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        #print(rows)
        keys_list=[]
        for each_key_tuple in rows:
            keys_list.append(each_key_tuple[0])
        return keys_list
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')           

def select_replication(server_name,replication):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        #print('Database connection open')  
        query="""select key,value from """+server_name+""" where replication="""+str(replication)+""";"""
        cur.execute(query)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        return(rows)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.') 
            
def set_replication(server_name,replication):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        #print('Database connection open')  
        query="""update """+server_name+""" set replication=-1 where replication="""+str(replication)+""";"""
        cur.execute(query)
        conn.commit()
        cur.close()
        return(1)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.') 
                                    
def replicate(server):
    if(server=="server1"):
        data=select_replication("server2",1)
        set_replication("server2",1)
    elif(server=="server2"):
        data=select_replication("server3",2)
        set_replication("server3",2)
    elif(server=="server3"):
        data=select_replication("server1",3)
        set_replication("server1",3)
    
    if(data!=[]):
        for i in data:
            key,value=i
            print(key,value)
            insert(key,value,server,-1)
    return data    
#replicate("server2")

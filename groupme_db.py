import psycopg2, time, os
from groupy.client import Client
from datetime import datetime, timezone
from sys import argv
import members #file with key value payers of group membes user IDs and first names

token = os.environ['gm_api_token']
client = Client.from_token(token)
group_id = os.environ['group_id']
group = client.groups.get(group_id)

db_password = os.environ['db_password']
connection_string = "dbname='gtbz_gm' user='postgres' password= '%s' host='localhost' port='5432'" % db_password

def create_table():
    conn=psycopg2.connect(connection_string)
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS messages (sender_id TEXT, created_at TEXT, msg_text TEXT, msg_id TEXT, likes INTEGER, created_at_ts TIMESTAMP)")
    conn.commit()
    conn.close()

def insert(sender_id, created_at, msg_text, msg_id, likes, created_at_ts):
    conn=psycopg2.connect(connection_string)
    cur=conn.cursor()
    cur.execute("INSERT INTO messages VALUES (%s, %s, %s, %s, %s, %s)",(sender_id, created_at, msg_text, msg_id, likes, created_at_ts))
    conn.commit()
    conn.close()

def get_msg(last_id):
    sender_id = message[0].sender_id
    created_at = message[0].data['created_at']
    msg_text = message[0].text
    msg_id = message[0].id
    likes = len(message[0].data['favorited_by'])
    ts = int(created_at)
    stamp = datetime.utcfromtimestamp(ts)
    return sender_id, created_at, msg_text, msg_id, likes, stamp

def view():
    conn=psycopg2.connect(connection_string)
    cur=conn.cursor()
    cur.execute("SELECT * FROM messages")
    rows=cur.fetchall()
    conn.close()
    return rows

def update_tz():
    count=0
    conn=psycopg2.connect(connection_string)
    cur=conn.cursor()
    cur.execute("SELECT * FROM messages")
    rows=cur.fetchall()
    for row in rows:
        ts = row[1]
        str_stamp = datetime.utcfromtimestamp(int(ts))
        cur.execute("UPDATE messages SET created_at_ts = %s WHERE created_at = %s AND created_at_ts is null",(str_stamp, ts))
        count += 1
        print('Total rows updated is ' + str(count))
    conn.commit()
    conn.close()


def update_name(user_id, user_name):
    conn=psycopg2.connect(connection_string)
    cur=conn.cursor()
    cur.execute("UPDATE messages SET sender_name = %s WHERE sender_id = %s",(user_name , user_id))
    conn.commit()
    conn.close()


def find_latest():
    conn=psycopg2.connect(connection_string)
    cur=conn.cursor()
    cur.execute('SELECT msg_id, MAX(created_at_ts) FROM messages GROUP BY msg_id')
    rows = cur.fetchall()
    current_count = len(rows)
    conn.close()
    last = rows[-1][0]
    return last, current_count

mem2 = dict((y,x) for x,y in members.items()) #reverses the key/value pairs of the member dictionary



def update_table():
    sp = find_latest()
    init_id = sp[0]
    count = 0
    start_time = datetime.now()

    while init_id != group.messages.list()[0].id:
       if count % 1000 == 0:
         print(count)
         cur_time = datetime.now()
         dif = cur_time - start_time
         minutes, seconds = int(dif.seconds/60), dif.seconds % 60
         print("The current update has been running for %ds minutes and %s secibds."  % (minutes, seconds))

       try:
           message = group.messages.list_after(init_id)
           conn=psycopg2.connect(connection_string)
           cur=conn.cursor()
           for i in message:
               sender_id = i.sender_id
               created_at = i.data['created_at']
               msg_text = i.text
               msg_id = i.id
               likes = len(i.data['favorited_by'])
               ts = int(created_at)
               stamp = datetime.utcfromtimestamp(ts)
               try:
                   sender_name = mem2[sender_id]
               except:
                   sender_name = 'system'
               cur.execute("INSERT INTO messages VALUES (%s, %s, %s, %s, %s, %s, %s)",(sender_id, created_at, msg_text, msg_id, likes, stamp, sender_name))
               count += 1
           conn.commit()
           conn.close()
           init_id = message[-1].id
       except IndexError:
           print('Index error at count' + str(count))
           continue
       except:
           time.sleep(2)
           pass
    tot = sp[1] + count
    print('Update complete. %s new records were added. The total number of messages is %s.' % (count, tot))

def main(argv):
    try:
        if argv[1] == 'update':
            update_table()
    except:
        pass

if __name__ == '__main__':
    main(argv)

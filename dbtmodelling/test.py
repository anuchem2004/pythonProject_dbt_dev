# import os.path
# import os
# import psycopg2
# import json
#
# with open('config.json', 'r') as json_file:
#
#     data=json.load(json_file)
#     hostname= data['hostname']
#     database= data['database']
#     username = data['username']
#     pwd= data['pwd']
#     port_id= data['port_id']
#     # print (hostname, datab
#     print(hostname, database, username, pwd, port_id)
#     conn = psycopg2.connect(host=hostname, database=database, user=username, password=pwd, port=port_id)
#     cur = conn.cursor()
#     cur.execute(f"create table anu (id int)")
#     conn.commit()
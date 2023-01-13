import psycopg2


try :
    cnx = psycopg2.connect(
        host= 'localhost',
        user= 'postgres',
        password ='postgres123',
        database= 'postgres',
        port = 5400

    )
    print (cnx)
    cur = cnx.cursor()
    cur.execute("create table testpy4(id int, name varchar(50))")
    cnx.commit()
except :
    print("Exception thrown. Connection does not exist.")

else:
    cur.close()
    cnx.close()
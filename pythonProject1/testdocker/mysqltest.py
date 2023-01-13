import mysql.connector
from mysql.connector import errorcode

try :
    cnx = mysql.connector.connect(
        host= "localhost",
        user= "admin",
        password ="admin123",
        database = 'sys',
        port = 3309

    )
    print (cnx)
except mysql.connector.Error as err:
    print("Exception thrown. Connection does not exist.")
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
  cnx.close()

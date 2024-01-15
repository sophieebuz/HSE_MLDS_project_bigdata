from mysql.connector import connect, Error
from configs import db_params

conn = connect(
            host="localhost",
            user=db_params['user'],
            password=db_params['password'],
            database='hse')

cursor = conn.cursor()

cursor.execute("SELECT * FROM bth_performances")
print(cursor.fetchall())


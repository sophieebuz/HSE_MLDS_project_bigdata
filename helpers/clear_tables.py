from mysql.connector import connect, Error


try:
    with connect(
        host="localhost",
        user='arhimag',
        password='password57',
        database='hse'
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE bth_performances")

            cursor.execute("TRUNCATE TABLE bth_notifications")
    

except Error as e:
    print(e)


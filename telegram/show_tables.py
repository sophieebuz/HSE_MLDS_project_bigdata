from mysql.connector import connect, Error


try:
    with connect(
        host="localhost",
        user='arhimag',
        password='password57',
        database='hse'
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            result = cursor.fetchall()
            for row in result:
                print(row)

            print('# --------------------------------------------------------- #')

            cursor.execute("select * from bth_performances")
            result = cursor.fetchall()
            for row in result:
                print(row)

            print('# --------------------------------------------------------- #')

            cursor.execute("select * from bth_notifications")
            result = cursor.fetchall()
            for row in result:
                print(row)

except Error as e:
    print(e)


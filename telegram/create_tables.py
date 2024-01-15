from mysql.connector import connect

db_params = {
    'user': 'arhimag',
    'password': 'password57',
}

conn = connect(
            host='localhost',
            user=db_params['user'],
            password=db_params['password'],
            database='hse')

cursor = conn.cursor()

# cursor.execute("SHOW TABLES")
# print(cursor.fetchall())

# cursor.execute("SELECT * FROM bth_performances")
# for elem in cursor.fetchall():
#     print(elem, sep='\n')

# cursor.execute("DROP TABLE bth_users")
# cursor.execute("DROP TABLE bth_subscribes")

# query1 = """CREATE TABLE bth_users (
#             chat_id BIGINT UNIQUE,
#             fisrt_name TEXT NOT NULL,
#             nickname TEXT,
#             birthday DATE NOT NULL,
#             PRIMARY KEY (chat_id))"""
# cursor.execute(query1)

# query2 = """CREATE TABLE bth_subscribes (
#             chat_id BIGINT NOT NULL,
#             perf_name VARCHAR(255),
#             day_of_week VARCHAR(255),
#             CONSTRAINT name_unique UNIQUE (chat_id, perf_name),
#             CONSTRAINT day_unique UNIQUE (chat_id, day_of_week))"""
# cursor.execute(query2)

# cursor.execute("SHOW TABLES")
# print(cursor.fetchall())
# cursor.execute("DESCRIBE bth_notifications")
# # print(cursor.fetchall())



# cursor.execute("INSERT INTO bth_notifications (date, time, name, message) VALUES ('13 января', '19:00', 'Щелкунчик', 'Появились билеты на спектакль')")
# conn.commit()

# # cursor.execute("SELECT * FROM bth_notifications")
# # for elem in cursor.fetchall():
# #     print(elem, sep='\n')

# cursor.close()
# conn.close()

# cursor.execute("DELETE FROM bth_notifications WHERE date='13 января' AND time='19:00' AND name='Щелкунчик'")
# conn.commit()
# cursor.close()
# conn.close()

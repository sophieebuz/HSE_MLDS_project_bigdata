from mysql.connector import connect
from telegram.telegram_bot import send_message


def send_push_messages(db_params):
     conn = connect(
            host='localhost',
            user=db_params['user'],
            password=db_params['password'],
            database='hse')

     cursor = conn.cursor()

     query_join_perf = """
                         SELECT p.date, p.day_of_week, p.type, p.name, p.age, p.time, p.scene, p.tickets, p.price, n.message, s.chat_id
                         FROM bth_notifications n
                         JOIN bth_performances p ON n.date = p.date AND n.time = p.time AND n.name = p.name 
                         JOIN bth_subscribes s ON n.name = s.perf_name
                       """

     query_join_day = """
                         SELECT p.date, p.day_of_week, p.type, p.name, p.age, p.time, p.scene, p.tickets, p.price, n.message, s.chat_id
                         FROM bth_notifications n
                         JOIN bth_performances p ON n.date = p.date AND n.time = p.time AND n.name = p.name 
                         JOIN bth_subscribes s ON p.day_of_week = s.day_of_week
                      """
     cursor.execute(query_join_perf)
     for elem in cursor.fetchall():
          print(elem)
          send_message(elem, 'Подписка на спектакль', elem[3:4][0])

     cursor.execute(query_join_day)
     for elem in cursor.fetchall():
          print(elem)
          send_message(elem, 'Подписка на день недели', elem[1:2][0])
 
     return

# cursor.execute("DELETE FROM bth_notifications WHERE date='13 января' AND time='19:00' AND name='Щелкунчик'")
# conn.commit()




# conn = connect(
#             host="localhost",
#             user=db_params['user'],
#             password=db_params['password'],
#             database='hse')

# cursor = conn.cursor()

# # cursor.execute("SELECT * FROM bth_performances")
# # for elem in cursor.fetchall():
# #     print(elem, sep='\n')

# cursor.execute("DELETE FROM bth_notifications WHERE date='13 января' AND time='19:00' AND name='Щелкунчик'")
# conn.commit()

# send_push_messages()

# cursor.execute("SELECT * FROM bth_notifications")
# for elem in cursor.fetchall():
#     print(elem, sep='\n')

# cursor.execute("SELECT * FROM bth_subscribes")
# for elem in cursor.fetchall():
#     print(elem, sep='\n')


# # query_join1 = """SELECT *
# #                 FROM bth_notifications n
# #                      JOIN bth_performances p ON n.date = p.date AND n.time = p.time AND n.name = p.name 
# #                      JOIN bth_subscribes s ON n.name = s.perf_name
# #                 """

# # query_join2 = """SELECT *
# #                 FROM bth_notifications n
# #                      JOIN bth_performances p ON n.date = p.date AND n.time = p.time AND n.name = p.name 
# #                      JOIN bth_subscribes s ON p.day_of_week = s.day_of_week
# #                 """

# query_join_perf = """SELECT p.date, p.day_of_week, p.type, p.name, p.age, p.time, p.scene, p.tickets, p.price, n.message, s.chat_id
#                      FROM bth_notifications n
#                      JOIN bth_performances p ON n.date = p.date AND n.time = p.time AND n.name = p.name 
#                      JOIN bth_subscribes s ON n.name = s.perf_name
#                 """

# query_join_day = """SELECT p.date, p.day_of_week, p.type, p.name, p.age, p.time, p.scene, p.tickets, p.price, n.message, s.chat_id
#                     FROM bth_notifications n
#                     JOIN bth_performances p ON n.date = p.date AND n.time = p.time AND n.name = p.name 
#                     JOIN bth_subscribes s ON p.day_of_week = s.day_of_week
#                 """

# dict_keys = ['date', 'day_of_week', 'type', 'name', 'age', 'time', 'scene', 'tickets', 'price', 'message', 'chat_id']
# push_message = {col: [] for col in dict_keys}

# cursor.execute(query_join_perf)
# for elem in cursor.fetchall():
#     for i in range(len(elem)):
#           push_message[dict_keys[i]].append(elem[i])

# print(push_message)

# cursor.execute(query_join_day)
# for elem in cursor.fetchall():
#      print(elem)
#      send_message(elem, 'Подписка на день недели', elem[1:2][0])

# cursor.execute(query_join_day)
# for elem in cursor.fetchall():
#     for i in range(len(elem)):
#         push_message[dict_keys[i]].append(elem[i])

# print(push_message)

# send_message(push_message=push_message)

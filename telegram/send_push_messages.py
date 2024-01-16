from mysql.connector import connect
from telegram.telegram_bot import send_message


def send_push_messages_to_telegram(db_params):
     with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as conn:
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
               send_message(elem, 'Подписка на спектакль', elem[3:4][0])

          cursor.execute(query_join_day)
          for elem in cursor.fetchall():
               send_message(elem, 'Подписка на день недели', elem[1:2][0])

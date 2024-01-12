import telebot
from telebot import types
import sqlite3
import datetime
import pandas as pd


# подключаем базу данных
conn = sqlite3.connect('bolshoi_theater.db')
cursor = conn.cursor()
# cursor.execute("DROP TABLE users")
# cursor.execute("DROP TABLE performances")


try:
    query1 = """CREATE TABLE \"users\" (
               \"fisrt_name\" TEXT NOT NULL,
               \"nickname\" TEXT UNIQUE,
               \"birthday\" DATE NOT NULL,
               PRIMARY KEY (\"nickname\"))"""
    cursor.execute(query1)

    df = pd.read_csv('data/dftest.csv')
    df.to_sql(name='performances', con=conn)
    conn.commit()

    # query2 = """CREATE TABLE \"performances\" (
    #            \"date\" TEXT NOT NULL,
    #            \"day_of_week\" TEXT NOT NULL,
    #            \"type\" TEXT NOT NULL,
    #            \"name\" TEXT NOT NULL,
    #            \"age\" TEXT NOT NULL,
    #            \"time\" TEXT NOT NULL,
    #            \"scene\" TEXT NOT NULL,
    #            \"tickets\" TEXT NOT NULL,
    #            \"price\" TEXT)"""
    # cursor.execute(query2)

except:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor.fetchall())

    cursor.execute("SELECT * FROM performances LIMIT 1")
    print(cursor.fetchall())
    # cursor.execute('DELETE FROM users')
    # conn.commit()
    # date = datetime.date(2000, 5, 23)
    # cursor.execute("INSERT INTO users (user_id, fisrt_name, nickname, birthday)\
    #                   VALUES (502830635, \"Sophie\", \"ethee_real\", \"2000-05-23\")")
    # conn.commit()
    # cursor.execute("SELECT * FROM users")
    # print(cursor.fetchall())
    pass


bot = telebot.TeleBot("6688259580:AAH6h-QJAtTleEF13bHpdVzLEK8MrDVjCh0")

@bot.message_handler(commands=['start'])
def send_start_message(message):
    text1 = f"Приветствую, {message.from_user.first_name}, в телеграм-боте об анонсах Большого театра! \n\n" \
            f"Здесь ты будешь получать уведомления о новых спектаклях, как только они появятся в продаже."
    text2 = "Если хочешь получать уведомления о конкретных спектаклях, надо пройти регистрацию"

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    itembtn1 = types.InlineKeyboardButton(text="Регистрация", callback_data='registry')
    keyboard.add(itembtn1)
    bot.send_message(message.from_user.id, text=text1)
    bot.send_message(message.from_user.id, text=text2, reply_markup=keyboard)
    # print(message.from_user.first_name, message.from_user.username, message.from_user.last_name)

@bot.callback_query_handler(func=lambda call:True)
def callback(call):
    if call.message:
        if call.data == 'registry':
            text = "Укажите год, месяц и день вашего рождения в формате гггг-мм-дд"
            msg = bot.send_message(call.message.chat.id, text=text)
            bot.register_next_step_handler(msg, callback_user_registry)

def callback_user_registry(call):
    check1 = call.text.split('-')
    now = datetime.date.today().year
    if int(check1[0]) > now or int(check1[1]) > 12 or int(check1[2]) > 31 or \
            (len(check1[0]) != 4) or (len(check1[1]) != 2) or (len(check1[2]) != 2):
        text = "Дата указана неправильно.\nУкажите год, месяц и день вашего рождения в формате гггг-мм-дд"
        msg = bot.send_message(call.chat.id, text=text)
        bot.register_next_step_handler(msg, callback_user_registry)
    else:
        try:
            with sqlite3.connect('bolshoi_theater.db') as con:
                cursor = con.cursor()
                cursor.execute('INSERT INTO users (fisrt_name, nickname, birthday)\
                          VALUES (?, ?, ?)', (call.from_user.first_name, call.from_user.username, call.text))
                con.commit()

                cursor.execute("SELECT * FROM users")
                print(cursor.fetchall())
            bot.send_message(call.chat.id, text="Вы успешно зарегистрированы!")
        except:
            bot.send_message(call.chat.id, text="Вы уже зарегистрированы!")

def get_plans_string(tasks):
    tasks_str = []
    for val in list(enumerate(tasks)):
        tasks_str.append(str(val[0] + 1) + ') ' + val[1][0] + '\n')
    return ''.join(tasks_str)

@bot.message_handler(commands=['list'])
def show_list_of_performances(message):
    with sqlite3.connect('bolshoi_theater.db') as con:
        cursor = con.cursor()
        cursor.execute("SELECT DISTINCT name FROM performances")
        tasks = get_plans_string(cursor.fetchall())
        bot.send_message(message.chat.id, text=tasks)


@bot.message_handler(commands=['help'])
def show_list_of_performances(message):
    text = "/start - начало работы с ботом, прохождение регистрации \n"\
           "/help - показать все доступные команды \n"\
           "/list - показать все доступные спектакли"
    bot.send_message(message.chat.id, text=text)

bot.polling(none_stop=True)

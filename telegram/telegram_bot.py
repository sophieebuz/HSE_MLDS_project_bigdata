from mysql.connector import connect
import telebot
from telebot import types
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--user', type=str, required=True, help="DB user")
parser.add_argument('--password', type=str, required=True, help="DB password")
parser.add_argument('--token', type=str, required=True, help="Telegram bot token")
args = parser.parse_args()


token = args.token

db_params = {
    'user': args.user,
    'password': args.password,
}


bot = telebot.TeleBot(token)

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

@bot.callback_query_handler(func=lambda call: call.data == 'registry')
def callback(call):
    text = "Укажите год, месяц и день вашего рождения в формате гггг-мм-дд"
    msg = bot.send_message(call.message.chat.id, text=text)
    bot.register_next_step_handler(msg, callback_user_registry)

def callback_user_registry(call):
    try:
        check1 = call.text.split('-')
        now = datetime.date.today().year
        if int(check1[0]) > now or int(check1[1]) > 12 or int(check1[2]) > 31 or \
                (len(check1[0]) != 4) or (len(check1[1]) != 2) or (len(check1[2]) != 2):
            text = "Дата указана неправильно.\nУкажите год, месяц и день вашего рождения в формате гггг-мм-дд"
            msg = bot.send_message(call.chat.id, text=text)
            bot.register_next_step_handler(msg, callback_user_registry)
        else:
            try:
                with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
                    cursor = con.cursor()
                    cursor.execute('INSERT INTO bth_users (chat_id, fisrt_name, nickname, birthday)\
                                    VALUES (%s, %s, %s, %s)', (call.chat.id, call.from_user.first_name, call.from_user.username, call.text))
                    con.commit()
                bot.send_message(call.chat.id, text="Вы успешно зарегистрированы!")
                bot.send_message(call.chat.id, text="Для того, чтобы подписаться на конкретные спектакли, введите команду /list")
            except:
                bot.send_message(call.chat.id, text="Вы уже зарегистрированы!")
                bot.send_message(call.chat.id, text="Если хотите подписаться на конкретные спектакли, введите команду /list")
    except:
        text = "Проверьте, что вы правильно указали дату рождения. Введите дату рождения правильно (в формате гггг-мм-дд)."
        msg = bot.send_message(call.chat.id, text=text)
        bot.register_next_step_handler(msg, callback_user_registry)

def get_plans_string(tasks):
    tasks_str = []
    for val in list(enumerate(tasks)):
        tasks_str.append(str(val[0] + 1) + ') ' + val[1][0] + '\n')
    return ''.join(tasks_str)


@bot.message_handler(commands=['list'])
def show_list_of_performances(message):
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        cursor.execute("SELECT DISTINCT name FROM bth_performances")
        tasks = get_plans_string(cursor.fetchall())

        keyboard = types.InlineKeyboardMarkup(row_width=1)
        itembtn1 = types.InlineKeyboardButton(text="Подписаться на спектакль", callback_data='subscribe_perf')
        itembtn2 = types.InlineKeyboardButton(text="Все спектакли на день недели", callback_data='subscribe_day')
        keyboard.add(itembtn1, itembtn2)
        bot.send_message(message.chat.id, text=tasks, reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data == 'subscribe_perf')
def callback_subscribe(call):
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        cursor.execute(f"SELECT chat_id FROM bth_users WHERE chat_id={call.message.chat.id}")
        check_reg = True if len(cursor.fetchall()) == 1 else False
    if check_reg == True:
        text = "Укажите номер спектакля, на который вы хотите подписаться"
        msg = bot.send_message(call.message.chat.id, text=text)
        bot.register_next_step_handler(msg, callback_added_subscribe_perf)
    else:
        text = "Чтобы получить возможно добавлять спектакль в подписку - зарегистируйтесь.\n" \
               "Для этого введите команду /start."
        bot.send_message(call.message.chat.id, text=text)

@bot.callback_query_handler(func=lambda call: call.data == 'subscribe_day')
def callback_subscribe(call):
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        cursor.execute(f"SELECT chat_id FROM bth_users WHERE chat_id={call.message.chat.id}")
        check_reg = True if len(cursor.fetchall()) == 1 else False
    if check_reg == True:
        text = "Выберете номер дня недели:\n\n " \
               "1. Понедельник\n " \
               "2. Вторник\n" \
               "3. Среда\n" \
               "4. Четверг\n" \
               "5. Пятница\n" \
               "6. Суббота\n" \
               "7. Воскресенье"

        msg = bot.send_message(call.message.chat.id, text=text)
        bot.register_next_step_handler(msg, callback_added_subscribe_day)
    else:
        text = "Чтобы получить возможно добавлять день недели в подписку - зарегистируйтесь.\n" \
               "Для этого введите команду /start."
        bot.send_message(call.message.chat.id, text=text)

def callback_added_subscribe_perf(call):
    try:
        with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
            cursor = con.cursor()
            cursor.execute("SELECT DISTINCT name FROM bth_performances")
            tasks = cursor.fetchall()
            check_correct_num = True if 1 <= int(call.text) <= len(tasks) else False
        if check_correct_num:
            perf = int(call.text)
            perf_name = list(enumerate(tasks))[perf - 1][1][0]
            try:
                with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
                    cursor = con.cursor()
                    cursor.execute('INSERT INTO bth_subscribes (chat_id, perf_name, day_of_week)\
                                    VALUES (%s, %s, %s)', (call.chat.id, perf_name, None))
                    con.commit()

                    text = f'Спектакль "{perf_name}" успешно добавлен в ваши анонсы!\n' \
                           f'Чтобы посмотреть, на какие спектакли вы подписаны, вызовите команду /mylist'
                    bot.send_message(call.chat.id, text=text)

            except:
                text = "Вы уже подписаны на данный спектакль.\nЕсли хотите изменить настройки " \
                       "вашей подписки вызовите команду /change_mylist."
                bot.send_message(call.chat.id, text=text)
        else:
            raise
    except:
        text = "Проверьте, что такой номер спектакля существует в списке. Введите номер правильно."
        msg = bot.send_message(call.chat.id, text=text)
        bot.register_next_step_handler(msg, callback_added_subscribe_perf)

def callback_added_subscribe_day(call):
    try:
        check_correct_num = True if 1 <= int(call.text) <= 7 else False
        if check_correct_num:
            num_day = int(call.text)
            days = {1: "понедельник", 2: "вторник", 3: "среда",
                    4: "четверг", 5: "пятница", 6: "суббота", 7: "воскресенье"}
            day = days[num_day]
            try:
                with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
                    cursor = con.cursor()
                    cursor.execute('INSERT INTO bth_subscribes (chat_id, perf_name, day_of_week)\
                                    VALUES (%s, %s, %s)', (call.chat.id, None, day))
                    con.commit()

                    text = f'"{day[:1].upper() + day[1:]}" успешно добавлен в ваши анонсы!\n' \
                        f'Чтобы посмотреть, о каких днях недели вы получаете уведомления, вызовите команду /mylist'
                    bot.send_message(call.chat.id, text=text)

            except:
                text = "Вы уже получаете уведомления о всех спектаклях, проходящих в этот день недели.\n" \
                       "Если хотите изменить настройки вашей подписки вызовите команду /change_mylist."
                bot.send_message(call.chat.id, text=text)
        else:
            raise
    except:
        text = "Проверьте, что такой номер дня недели существует в списке. Введите номер правильно."
        msg = bot.send_message(call.chat.id, text=text)
        bot.register_next_step_handler(msg, callback_added_subscribe_day)

@bot.message_handler(commands=['mylist'])
def show_person_list(message):
    chat_id = message.chat.id
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        query1 = "SELECT perf_name FROM bth_subscribes WHERE chat_id={} and perf_name IS NOT NULL".format(chat_id)
        cursor.execute(query1)
        tasks1 = get_plans_string(cursor.fetchall())
        query2 = "SELECT day_of_week FROM bth_subscribes WHERE chat_id={} and day_of_week IS NOT NULL".format(chat_id)
        cursor.execute(query2)
        tasks2 = get_plans_string(cursor.fetchall())
    text = "*Спектакли:*\n" + tasks1 + "\n*Дни недели:*\n" + tasks2
    bot.send_message(message.chat.id, text=text, parse_mode="Markdown")

@bot.message_handler(commands=['change_mylist'])
def change_person_list(message):
    text = "Чтобы бы вы хотели изменить в вашей подписке?"
    keyboard = types.ReplyKeyboardMarkup(row_width=2)
    itembtn1 = types.KeyboardButton('Отписаться от спектакля')
    itembtn2 = types.KeyboardButton('Отписаться от дня недели')
    keyboard.add(itembtn1, itembtn2)
    msg = bot.send_message(message.chat.id,
                     text=text, reply_markup=keyboard)
    bot.register_next_step_handler(msg, callback_change_person_list)

def delete_perf(call):
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        query1 = "SELECT perf_name FROM bth_subscribes WHERE chat_id={} and perf_name IS NOT NULL".format(call.chat.id)
        cursor.execute(query1)
        tasks1 = get_plans_string(cursor.fetchall())

    text = "Укажите номер спектакля, который хотите удалить\n\n" + tasks1
    msg = bot.send_message(call.chat.id, text=text)
    bot.register_next_step_handler(msg, delete_perf_)

def delete_perf_(msg):
    perf = int(msg.text)
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        cursor.execute("SELECT perf_name FROM bth_subscribes WHERE chat_id={} and perf_name IS NOT NULL".format(msg.chat.id))
        tasks = cursor.fetchall()
        perf_name = list(enumerate(tasks))[perf - 1][1][0]

        query = f"DELETE FROM bth_subscribes WHERE chat_id={msg.chat.id} AND perf_name='{perf_name}'"
        cursor.execute(query)
        con.commit()
        bot.send_message(msg.chat.id, text=f'Спектакль "{perf_name}" удален из подписки. Больше вы не будете получать о нем уведомлений')

def delete_day_of_week(call):
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        query2 = "SELECT day_of_week FROM bth_subscribes WHERE chat_id={} and day_of_week IS NOT NULL".format(call.chat.id)
        cursor.execute(query2)
        tasks2 = get_plans_string(cursor.fetchall())

    text = "Укажите день недели, который хотите удалить\n\n" + tasks2
    msg = bot.send_message(call.chat.id, text=text)
    bot.register_next_step_handler(msg, delete_day_of_week_)

def delete_day_of_week_(msg):
    num_day = int(msg.text)
    with connect(host="localhost", user=db_params['user'], password=db_params['password'], database='hse') as con:
        cursor = con.cursor()
        cursor.execute(f"SELECT day_of_week FROM subscribes WHERE chat_id={msg.chat.id} and day_of_week IS NOT NULL")
        tasks = cursor.fetchall()
        day = list(enumerate(tasks))[num_day - 1][1][0]

        query = f"DELETE FROM bth_subscribes WHERE chat_id={msg.chat.id} AND day_of_week='{day}'"
        cursor.execute(query)
        con.commit()
        bot.send_message(msg.chat.id, text=f'"{day}" удален(-a) из подписки. Больше вы не будете получать уведомления по этому дню')


def callback_change_person_list(call):
    if call.text == "Отписаться от спектакля":
        try:
            delete_perf(call)
        except:
            bot.send_message(call.chat.id, 'Что то пошло не так')

    elif call.text == "Отписаться от дня недели":
        try:
            delete_day_of_week(call)
        except:
            bot.send_message(call.chat.id, 'Что то пошло не так')

# ---------------------------------------------------------
def get_message_string(info_general, info_added, subscribe, type_of_subs):
    tasks_str = []
    tasks_str.append(f'*{subscribe}: {type_of_subs}*' + '\n')
    for val in list(info_general):
        tasks_str.append(str(val) + '\n')
    tasks_str.append('\n' + f'*{info_added[0]}*')
    return ''.join(tasks_str)


def send_message(push_message, subscribe, type_of_subs):
    message_text = get_message_string(push_message[:-2], push_message[-2:-1], subscribe, type_of_subs)
    bot.send_message(chat_id=push_message[-1], text=message_text, parse_mode="Markdown")
# ---------------------------------------------------------

@bot.message_handler(commands=['help'])
def show_list_of_performances(message):
    text = "/start - начало работы с ботом, прохождение регистрации \n"\
           "/help - показать все доступные команды \n"\
           "/list - показать все доступные спектакли \n"\
           "/mylist - показать мои подписки на спектакли \n"\
           "/change_mylist - изменить подписку"
    bot.send_message(message.chat.id, text=text)

if __name__ == '__main__':
    print('Telegram bot started...')
    bot.polling(none_stop=True)

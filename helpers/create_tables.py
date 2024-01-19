from mysql.connector import connect, Error

db_params = {
    'user': 'arhimag',
    'password': 'password57',
}

conn = connect(
            host='localhost',
            user=db_params['user'],
            password=db_params['password'],
            database='hse'
        )

cursor = conn.cursor()

try:
    query_users = """
            CREATE TABLE bth_users (
                chat_id BIGINT UNIQUE,
                fisrt_name TEXT NOT NULL,
                nickname TEXT,
                birthday DATE NOT NULL,
                PRIMARY KEY (chat_id)
            )
            """
    cursor.execute(query_users)

    query_subscribes = """
            CREATE TABLE bth_subscribes (
                chat_id BIGINT NOT NULL,
                perf_name VARCHAR(255),
                day_of_week VARCHAR(255),
                CONSTRAINT name_unique UNIQUE (chat_id, perf_name),
                CONSTRAINT day_unique UNIQUE (chat_id, day_of_week)
            )
            """
    cursor.execute(query_subscribes)

    query_performances = """
            CREATE TABLE bth_performances (
                date TEXT NOT NULL,
                day_of_week TEXT,
                type TEXT,
                name TEXT NOT NULL,
                age TEXT,
                time TEXT NOT NULL,
                scene TEXT,
                tickets TEXT,
                price TEXT,
                tickets_num INT,
                min_price INT,
                max_price INT
            )
            """
    cursor.execute(query_performances)

    query_notifications = """
            CREATE TABLE bth_notifications (
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                name TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
    cursor.execute(query_notifications)
except:
    Error

cursor.close()
conn.close()

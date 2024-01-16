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
    query1 = """
            CREATE TABLE bth_users (
                chat_id BIGINT UNIQUE,
                fisrt_name TEXT NOT NULL,
                nickname TEXT,
                birthday DATE NOT NULL,
                PRIMARY KEY (chat_id)
            )
            """
    cursor.execute(query1)

    query2 = """
            CREATE TABLE bth_subscribes (
                chat_id BIGINT NOT NULL,
                perf_name VARCHAR(255),
                day_of_week VARCHAR(255),
                CONSTRAINT name_unique UNIQUE (chat_id, perf_name),
                CONSTRAINT day_unique UNIQUE (chat_id, day_of_week)
            )
            """
    cursor.execute(query2)
except:
    Error

cursor.close()
conn.close()

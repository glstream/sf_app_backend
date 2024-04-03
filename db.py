import psycopg2
import os


def get_db():
    host = os.getenv("host")
    dbname = os.getenv("dbname")
    user = os.getenv("user")
    password = os.getenv("password")
    sslmode = os.getenv("sslmode")
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(
        host, user, dbname, password, sslmode
    )
    db = psycopg2.connect(conn_string)
    try:
        yield db
    finally:
        db.close()

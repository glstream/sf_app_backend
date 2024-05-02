import asyncpg
import os

pool = None

async def init_db_pool():
    global pool
    host = os.getenv("host")
    dbname = os.getenv("dbname")
    user = os.getenv("user")
    password = os.getenv("password")
    sslmode = os.getenv("sslmode")
    pool = await asyncpg.create_pool(
        host=host,
        database=dbname,
        user=user,
        password=password,
        ssl=sslmode
    )

async def get_db():
    global pool
    if pool is None:
        await init_db_pool()
    async with pool.acquire() as connection:
        yield connection

async def close_db():
    global pool
    await pool.close()

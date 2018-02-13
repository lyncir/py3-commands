# -*- coding: utf-8 -*-
import asyncio

import aiofiles
import aiomysql


async def raw_sql(filename):
    """
    get raw sql. comment with `--`

    :param filename: filename
    """
    lines = []

    async with aiofiles.open(filename) as f:
        async for line in f:
            # comment with `--`
            if not line.startswith('--'):
                # filter space line
                if line.strip():
                    lines.append(line.strip())

    return "".join(lines)


def printt(my_dict, col_list=None):
    """ Pretty print a list of dictionaries (my_dict) as a dynamically sized table.
    If column names (col_list) aren't specified, they will show in random order.
    Author: Thierry Husson - Use it as you want but don't blame me.

    :param my_dict: dict list
    :param col_list: display columns will you want
    """
    if not col_list:
        col_list = list(my_dict[0].keys() if my_dict else [])

    # 1st row = header
    my_list = [col_list]

    for item in my_dict:
        my_list.append([str(item[col] or '') for col in col_list])

    col_size = [max(map(len, col)) for col in zip(*my_list)]
    format_str = ' | '.join(["{{:<{}}}".format(i) for i in col_size])

    # Seperating line
    my_list.insert(1, ['-' * i for i in col_size])
    for item in my_list:
        print(format_str.format(*item))


async def execute_sql(loop, host, port, user, passwd, db, sql_file):
    """
    execute sql

    :param loop: asyncio event loop
    :param host: host
    :param port: port
    :param user: user
    :param passwd: password
    :param db: database name
    :param sql_file: raw sql filename
    """

    conn = await aiomysql.connect(host=host, port=port,
                                  user=user, password=passwd,
                                  db=db,
                                  cursorclass=aiomysql.DictCursor,
                                  loop=loop)

    cur = await conn.cursor()
    sql = await raw_sql(sql_file)
    await cur.execute(sql)

    more = True
    while more:
        data = await cur.fetchall()
        if data:
            printt(data)
        more = await cur.nextset()


if __name__ == '__main__':
    host = '127.0.0.1'
    port = 3306
    user = 'root'
    passwd = ''
    db_name = 'test'

    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute_sql(loop, host, port,
                                        user, passwd,
                                        db_name, 'raw_my.sql'))

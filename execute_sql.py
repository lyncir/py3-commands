# -*- coding: utf-8 -*-
import asyncio

import aiofiles
import aiomysql
import aiopg
import psycopg2


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

    return " ".join(lines)


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
        my_list.append([str(item[col]) for col in col_list])

    col_size = [max(map(len, col)) for col in zip(*my_list)]
    format_str = ' | '.join(["{{:<{}}}".format(i) for i in col_size])

    # Seperating line
    my_list.insert(1, ['-' * i for i in col_size])
    for item in my_list:
        print(format_str.format(*item))


async def execute_sql(loop, db_type, db_kwargs, sql):
    """
    execute sql

    :param loop: asyncio event loop
    :param db_type: mysql or postgres
    :param db_kwargs: database connect params
    :param sql_file: raw sql filename
    """
    hostname = db_kwargs.get('hostname')
    port = db_kwargs.get('port')
    username = db_kwargs.get('username')
    password = db_kwargs.get('password')
    dbname = db_kwargs.get('dbname')

    if db_type == "postgres":
        conn = await aiopg.connect(host=hostname,
                                   port=port,
                                   user=username,
                                   password=password,
                                   database=dbname,
                                   enable_hstore=False,
                                   cursor_factory=psycopg2.extras.RealDictCursor,
                                   loop=loop)

    elif db_type == 'mysql':
        conn = await aiomysql.connect(host=hostname,
                                      port=port,
                                      user=username,
                                      password=password,
                                      db=dbname,
                                      cursorclass=aiomysql.DictCursor,
                                      charset='utf8',
                                      loop=loop)

    else:
        raise Exception("Database type not specified! Must select one of: postgres, mysql")

    cur = await conn.cursor()
    await cur.execute(sql)

    results = []
    if db_type == 'mysql':

        # multi sql
        more = True
        while more:
            data = await cur.fetchall()
            if data:
                results.append(data)
            more = await cur.nextset()

        # close
        await cur.close()
        conn.close()

    elif db_type == "postgres":
        data = await cur.fetchall()
        if data:
            results.append(data)

        # close
        await conn.close()

    return results

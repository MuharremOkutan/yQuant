#! /usr/bin/python3

import asyncio
import os
import sqlite3
import zmq

from os import getpid
from collections import defaultdict
from logger import get_logger

def main():
    get_logger().info("Starting to store intraday price (pid:{})".format(getpid()))

    global socket
    global conn
    global cursor
    global SQL
    global lastest_prices

    PORT = 5001
    DB = "yQuant.db"
    SQL = "INSERT INTO intraday (symbol, price) VALUES ('{}', {})"
    lastest_prices = defaultdict(lambda: '')

    try:
        get_logger().info("Connecting to a price fetcher on port {}".format(PORT))
        socket = zmq.Context().socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, '')
        socket.connect("tcp://127.0.0.1:{}".format(PORT))

        get_logger().debug('Connecting to database, {}'.format(DB))
        conn = sqlite3.connect(DB)
        cursor = conn.cursor()
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA journal_mode = OFF")

        row = cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='intraday'").fetchone()
        if row is None:
            cursor.execute("CREATE TABLE intraday (datetime DATE DEFAULT (datetime('now', 'localtime')), symbol TEXT, price INTEGER)")
            cursor.execute("CREATE INDEX idx ON intraday (datetime)")

        loop = asyncio.get_event_loop()

        loop.create_task(receive())

        get_logger().debug('Entering an event loop')
        loop.run_forever()

    except KeyboardInterrupt:
        get_logger().debug('Stopping the event loop by keyboard interrupt')
        loop.stop()

    except Exception as e:
        get_logger().warn(repr(e))
        raise

    finally:
        get_logger().debug('Closing the event loop')
        loop.close()

        get_logger().debug('Closing the database conncection')
        conn.close()

        get_logger().debug('Closing the socket on port {}'.format(PORT))
        socket.close()

async def receive():
    global socket
    global lastest_prices

    message = socket.recv_string()
    # get_logger().debug("Message received: {}".format(message))

    symbol, price = message.split('=')
    if lastest_prices[symbol] != price:
        asyncio.get_event_loop().create_task(store(symbol, price))
    lastest_prices[symbol] = price

    asyncio.get_event_loop().create_task(receive())

async def store(symbol, price):
    global conn
    global cursor
    global SQL

    sql = SQL.format(symbol, price)
    # get_logger().debug("A query executing: {}".format(sql))

    cursor.execute(sql)
    conn.commit()
"""
    with open('config.json') as file:
        config = json.load(file)['mysql']

    start = time.time()
    get_logger().debug(
        'Connecting to {3} database {1}@{0}:{2}'.format(
            config['host'], config['user'], config['port'], config['db']))
    try:
        conn = pymysql.connect(host=config['host'],
                               port=config['port'],
                               user=config['user'],
                               password=config['password'],
                               db=config['db'])
        cursor = conn.cursor()
    except:
        get_logger().warn('Failed to connect database')
        raise

    affected_row = 0
    while True:
        message = socket.recv_string()
        # get_logger().debug(
        #   "store_price recevied a message ({})".format(message))
        if '[EOD]' in message:
            get_logger().debug("Market closed")
            break

        symbol = message.split('=')[0]
        price = int(message.split('=')[1])

        query = "INSERT INTO price_intraday (symbol, price) VALUES ('{}', {})".format(
                symbol, price)
        # get_logger().debug(query)
        cursor.execute(query)
        affected_row += cursor.rowcount

        if affected_row % 100 == 0:
            get_logger().debug("{0} rows are inserted".format(affected_row))
        # get_logger().debug("{0} rows are inserted ({1:.5g}) seconds)".format(cursor.rowcount, time.time() - start))
        conn.commit()
        # insert_values.append("('{}', {})".format(symbol, price))

        # if len(insert_values) == 100:
        #   start = time.time()
        #   cursor.execute(
        #       "INSERT INTO price_intraday (symbol, price) VALUES " + ", ".join(insert_values))
        #   get_logger.debug("{0} rows were inserted ({1:.5g}) seconds)".format(cursor.rowcount, time.time() - start))
        #   insert_values = []

    get_logger().debug("{0} rows were inserted ({1:.5g}) seconds)".format(affected_row, time.time() - start))
    conn.close()
    """

if __name__ == '__main__':
    main()


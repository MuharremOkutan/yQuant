import requests
import sqlite3

from bs4 import BeautifulSoup

from yQuant.logger import get_logger


def is_required_to_run():
    with sqlite3.connect("code.db") as con:
        cursor = con.cursor()
        row = cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='Master'").fetchone()
        return not row


def insert_from_koscom(cursor, type, query):
    url = 'http://datamall.koscom.co.kr/servlet/infoService/SearchIssue'
    output = requests.post(url, data=query)
    soup = BeautifulSoup(output.text, 'html.parser')
    options = soup.find(attrs={'name': 'stockCodeList'}).find_all('option')

    get_logger().debug('코스콤에서 {} {}개의 종목코드를 파싱했습니다.'.format(type, len(options)));

    for item in options:
        code = item.text[1:7] + '.KS'  # .KS is prepared to query yahoo
        name = item.text[8:]
        if '폐지' in name: continue
        _insert(cursor, type, code, name)


def _insert(cursor, type, code, name):
    if cursor.execute("SELECT code FROM Master WHERE code = :code", {'code': code}).fetchone() is not None:
        return

    cursor.execute("INSERT INTO Master (code, name, type, last_update) VALUES (:code, :name, :type, '2016-06-01')",
                   {'code': code, 'name': name, 'type': type})
    get_logger().debug('{} {} 종목을 Master 테이블에 추가했습니다.'.format(type, name))


def get_code_list():
    result = dict()
    with sqlite3.connect("code.db") as con:
        for code, name in con.cursor().execute("SELECT code, name FROM Master"):
            result[code] = name

    return result.items()


def run():
    with sqlite3.connect("code.db") as con:
        cursor = con.cursor()

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS Master (code TEXT, name TEXT, type TEXT, last_price INT, last_volume INT, last_update TEXT)")

        insert_from_koscom(cursor, "KOSPI200", {'marketBit': 1, 'where': 1, 'whereCode': 51})
        insert_from_koscom(cursor, "대형주", {'marketBit': 1, 'where': 1, 'whereCode': 2})
        insert_from_koscom(cursor, "중형주", {'marketBit': 1, 'where': 1, 'whereCode': 3})
        # insert_from_koscom(cursor, "소형주", {'marketBit': 1, 'where': 1, 'whereCode': 4})
        insert_from_koscom(cursor, "ETF", {'marketBit': 1, 'stdCodeName': 'KODEX'})
        insert_from_koscom(cursor, "ETF", {'marketBit': 1, 'stdCodeName': 'TIGER'})
        insert_from_koscom(cursor, "ETF", {'marketBit': 1, 'stdCodeName': 'KOSEF'})

        # insert(cursor, "통화", "DEXKOUS", "USD")
        # insert(cursor, "지수", "^KS11", "KOSPI")


if __name__ == '__main__':
    run()

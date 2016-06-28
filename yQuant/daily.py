from datetime import datetime
from datetime import timedelta
import sqlite3

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError

from yQuant.logger import get_logger
from yQuant.code import get_code_list
from yQuant.settings import FETCHING_START


def run():
    with sqlite3.connect("daily.db") as con:
        cursor = con.cursor()

        def _get_recent_date(cursor, table_name):
            row = cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='{}'".format(table_name)).fetchone()
            if row is None: return None

            row = cursor.execute("SELECT MAX(Date) FROM '{}'".format(table_name)).fetchone()
            return datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')

        for code, name in get_code_list():
            table_name = code

            recent_date = _get_recent_date(cursor, table_name)
            if recent_date is None: recent_date = FETCHING_START

            # 오늘 가져온 가격정보가 있다면 새로 가져오지 않음
            if recent_date.date() == datetime.now().date(): continue

            # 주말에는 새로운 정보가 없으므로 월요일 16시 이전까진 새로 가져오지 않음
            if recent_date.weekday() == 4 and datetime.now() - recent_date < timedelta(hours=64): continue

            feeder = 'fred' if 'DEX' in code else 'yahoo' # fred : DEXKOUS 원화USD DEXJPUS 엔화USD
            get_logger().debug("{} ({}) 종목의 {:%Y-%m-%d} 이후 주가정보를 {}에서 가져옵니다.".format(name, code, recent_date, feeder))

            # 최근에 저장된 데이터의 다음 날부터 오늘까지
            start_date = (recent_date + timedelta(days=1))
            end_date = datetime.now()

            # 단, 16시 이전에는 주가정보가 바뀔 수 있으므로 어제까지 자료만 가져옴
            if datetime.now().hour < 16: end_date = datetime.now() - timedelta(days=1)

            try:
                df = web.DataReader(code, feeder, start_date, end_date)
                # get_logger().debug("{} 종목의 새로운 주가정보가 {}일치 있습니다.".format(name, len(df)))
                if len(df) == 0:
                    get_logger().debug("{} ({}) 종목의 새로운 주가정보가 없습니다.".format(name, code))
                else:
                    df.to_sql(table_name, con, if_exists='append', chunksize=1000)
                    get_logger().debug("{} ({}) 종목의 {}일치 주가정보를 {} 테이블에 저장했습니다.".format(name, code, len(df), table_name))

            except RemoteDataError:
                get_logger().warn("{} ({}) 종목의 주가정보를 가져오는데 실패했습니다.".format(name, code))

            #cursor.execute("UPDATE Pool SET last_update = :date WHERE code = :code", {'code': code, 'date': today})

if __name__ == '__main__':
    run()
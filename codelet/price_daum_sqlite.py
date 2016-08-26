#! /usr/bin/python3

import json
import datetime
import re
import requests
import time
import sys
import sqlite3

from collections import defaultdict
from logger import get_logger 

def snapshot(prices):
	try:
		conn = sqlite3.connect("intraday.db")
		cursor = conn.cursor()
		cursor.execute("PRAGMA synchronous = OFF")
		cursor.execute("PRAGMA journal_mode = OFF")
		get_logger().debug('Database 연결 성공')

	except:
		get_logger().warn('Database 연결 실패')
		raise
	
	URL = "http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S"
	global is_market_finished 
	
	# parse(r.headers['Date']) 할 수 있으나, 다음 웹서버 시간이 부정확
	time = datetime.datetime.now()

	r = requests.get(URL)
	r.encoding = 'UTF-8'

	# 정규표현식으로 파싱(키값에 따옴표가 없어서 JSON 파싱 불가)
	# example : , {code:"095570",name :"AJ네트웍스",cost :"34,650",updn :"▲100",rate :"+0.29%"}	
	pattern = "code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn"
	rep = re.compile(pattern)

	for line in r.text.splitlines():
		if '장종료' in line:
			is_market_finished = True
		if 'code' not in line: 
			continue
		
		# print(line)
		match = rep.search(line)
		symbol = match.group(1)
		name = match.group(2)
		price = match.group(3).replace(',', '')

		# 직전 가격과 같으면 INSERT 생략		
		if(prices[symbol] == price):
			continue
		
		prices[symbol] = price
		
		row = cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='KS_{}'".format(symbol)).fetchone()
		if row is None:
			cursor.execute("CREATE TABLE KS_{} (datetime INTEGER, price INTEGER)".format(symbol))
			
		sql = "INSERT INTO KS_{} (datetime, price) VALUES (CURRENT_TIMESTAMP, '{}')"

		cursor.execute(sql.format(symbol, price))

	conn.close()
	
def main():
	global is_market_finished
	prices = defaultdict(list)
	interval = 5 if len(sys.argv) == 1 else int(sys.argv[1]) 
	
	#for i in range(2):
	while True:
		#if datetime.datetime.now().hour <   9: continue

		snapshot(prices)
		
		if(is_market_finished):
			get_logger().info("장이 종료되었습니다.")
			break
	
		time.sleep(interval)
	
if __name__ == '__main__':
	main()
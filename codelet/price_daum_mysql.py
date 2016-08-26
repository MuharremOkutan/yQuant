#! /usr/bin/python3

import json
import datetime
import re
import requests
import time
import sys
import pymysql

from collections import defaultdict
from logger import get_logger 

def snapshot(prices):
	with open('config.json') as file:
		config = json.load(file)['mysql']
	
	try:
		conn = pymysql.connect(host=config['host'],
							port=config['port'],
							user=config['user'],
							password=config['password'],
							db=config['db'])
		cursor = conn.cursor()
		
		get_logger().debug('Database 연결 성공')

	except pymysql.err.OperationalError:
		get_logger().warn('Database 연결 실패')
		raise
	
	URL = "http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S"
	sql = "INSERT INTO `price_intraday` (`symbol`, `price`) VALUES (%s, %s)"
	
	# parse(r.headers['Date']) 할 수 있으나, 다음 웹서버 시간이 부정확
	time = datetime.datetime.now()

	r = requests.get(URL)
	r.encoding = 'UTF-8'

	# 정규표현식으로 파싱(키값에 따옴표가 없어서 JSON 파싱 불가)
	# example : , {code:"095570",name :"AJ네트웍스",cost :"34,650",updn :"▲100",rate :"+0.29%"}	
	pattern = "code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn"
	rep = re.compile(pattern)

	for line in r.text.splitlines():
		if 'code' not in line: continue
		
		# print(line)
		match = rep.search(line)
		symbol = match.group(1)
		name = match.group(2)
		price = match.group(3).replace(',', '')

		# 직전 가격과 같으면 INSERT 생략		
		if(prices[symbol] == price):
			continue
		
		prices[symbol] = price
		cursor.execute(sql, (symbol, price))
		
	conn.commit()
	conn.close()
	
def snapshot():
	prices = defaultdict(list)
	interval = 60 if len(sys.argv) == 1 else int(sys.argv[1]) 
	
	for i in range(2):
	#while True:
	#	if datetime.datetime.now().hour <   9: continue
	#	if datetime.datetime.now().hour >= 15: break

		snapshot(prices)
		time.sleep(interval)

	# if len(stocks) > 0:
		# pickle.dump(stocks, open('intraday.{}s.{}'.format(interval, datetime.datetime.now().strftime('%y%m%d')), 'wb'))
	
		print(stocks['006840'])
	#print(stocks)
	
if __name__ == '__main__':
	main()
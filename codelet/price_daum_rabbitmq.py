#! /usr/bin/python3

import pika
import json
import datetime
import re
import requests
import time
import sys

from collections import defaultdict
from logger import get_logger 


def main():
	with open('config.json') as file:
		config = json.load(file)['rabbitmq']
	
	credentials = pika.PlainCredentials(config['user'], config['passwd'])
	parameters = pika.ConnectionParameters(config['host'], config['port'], '/', credentials)

	try:
		#get_logger().debug("Connecting to RabbitMQ ({}:{})".format(config['host'], config['port']))
		connection = pika.BlockingConnection(parameters)
	except pika.exceptions.ConnectionClosed:
		get_logger().warn("RabbitMQ Connection Error ({}:{})".format(config['host'], config['port']))
		sys.exit(1)

	channel = connection.channel()
	channel.exchange_declare(exchange="price", type="topic")
						
	stocks = defaultdict(list)
	URL = "http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S"

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
		
		
		channel.basic_publish(exchange='price',
							  routing_key=symbol,
							  body=price)
							  # 헤더에 시간 추가
	
	connection.close()	

		# 직전 가격과 같으면 continue
		#if len(stocks[symbol]) > 1 and stocks[symbol][-1][1] != price: 
		#	continue
		# stocks[symbol].append((time, price))


	#interval = 60 if len(sys.argv) == 1 else int(sys.argv[1]) 
	
	#for i in range(3):
	#while True:
	#	if datetime.datetime.now().hour <   9: continue
	#	if datetime.datetime.now().hour >= 15: break

	#	parse_price(stocks)
	#	time.sleep(interval)

	# if len(stocks) > 0:
		# pickle.dump(stocks, open('intraday.{}s.{}'.format(interval, datetime.datetime.now().strftime('%y%m%d')), 'wb'))
	
	#print(stocks['006840'])
	#print(stocks)

	
if __name__ == '__main__':
	main()
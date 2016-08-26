#! /usr/bin/python3

import aiohttp
import asyncio
import json
import re
from os import getpid
import zmq
from collections import defaultdict
from logger import get_logger

def main(): 
	get_logger().info("Starting to fetch intraday price from http://finance.daum.net (pid:{})".format(getpid()))

	global socket
	PORT = 5001

	URLs = {'KOSPI' : 'http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S',
			'KOSDAQ': 'http://finance.daum.net/xml/xmlallpanel.daum?stype=Q&type=S'}

	try:
		socket = zmq.Context().socket(zmq.PUB)
		get_logger().info("Binding a socket to publish price message on port {}".format(PORT))
		socket.bind("tcp://127.0.0.1:{}".format(PORT))

		loop = asyncio.get_event_loop()
		# loop.set_debug(is_debug())

		get_logger().debug('Preparing {} fetching tasks for event loop'.format(len(URLs.items())))
		[loop.create_task(fetch(market, url)) for market, url in URLs.items()]

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
		
		get_logger().debug('Closing the socket on port {}'.format(PORT))
		socket.close()

async def fetch(market, url):
	get_logger().debug("Starting fetching task for {}".format(market))

	INTERVAL = 2

	text = await request(url)
	is_market_closed, prices = await parse(text)
	await send(prices)

	if is_market_closed:
		get_logger().info("{} market is closed".format(market))
		if len(asyncio.Task.all_tasks()) < 2:
			asyncio.get_event_loop().stop()
	else:
		get_logger().debug("Another fetching task will be added after {} seconds".format(INTERVAL))
		await asyncio.sleep(INTERVAL)
		asyncio.get_event_loop().create_task(fetch(market, url))


async def request(url):
	get_logger().debug("Requesting {}".format(url))

	TIMEOUT = 1

	try:
		with aiohttp.Timeout(TIMEOUT):
			async with aiohttp.request('GET', url) as resp:
				#get_logger().debug(resp.status)
				assert resp.status == 200
				#get_logger().debug(await resp.text(encoding='utf8'))
				return await resp.text(encoding='utf8')

	except asyncio.TimeoutError:
		get_logger().warn("Timeout {} seconds for requesting".format(TIMEOUT))
		return ""


async def parse(text):
	get_logger().debug("Parsing {:,d} bytes".format(len(text)))
	
	is_market_closed = False

	# 정규표현식으로 파싱(Daum 데이터의 키값에 따옴표가 없어서 JSON 파싱 불가)
	# , {code:"095570",name :"AJ네트웍스",cost :"34,650",updn :"▲100",rate :"+0.29%"}
	rep = re.compile("code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn")

	prices = defaultdict()
	for line in text.splitlines():
		if '장종료' in line:
			is_market_closed = True 
			
		if 'code' not in line:
			continue

		match = rep.search(line)
		symbol = match.group(1)
		name = match.group(2)
		price = int(match.group(3).replace(',', ''))
		prices[symbol] = price

	return is_market_closed, prices

async def send(prices):
	get_logger().debug("Sending prices of {:,d} stocks".format(len(prices.items())))

	global socket

	#message = json.dumps(prices)
	#socket.send_string(message)

	for symbol, price in prices.items():
	 	#get_logger().debug("Sending a price message {}".format(message))
	 	message = "{}={}".format(symbol, price)
	 	socket.send_string(message)
	
if __name__ == '__main__':
	main()

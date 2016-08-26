from subprocess import call
from multiprocessing import Process
import sub

def main():
	print('main start')
	p = Process(target=sub.main)
	p.start()
	print('main end')

if __name__ == '__main__':
	main()

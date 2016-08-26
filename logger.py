import logging

_logger_initialized = False

def get_logger(name='yQuant'):
	_logger = logging.getLogger(name)

	if not _logger_initialized: 
		initialize(_logger)
	return _logger

def initialize(logger):
	global _logger_initialized

	logger.setLevel(logging.DEBUG)
	console_handler = logging.StreamHandler()
	console_handler.setFormatter(
		logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')
    )
	logger.addHandler(console_handler)

	# logger.addHandler(날짜별 logfile)
	# logger.addHandler(telegram)
	
	_logger_initialized = True


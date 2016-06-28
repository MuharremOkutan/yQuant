import yQuant.logger

# Signleton 으로 구현
class Broker:
    def __init__(self):
        logger = yQuant.logger.get_logger()
        logger.debug("Broker 시작")

        # 로그인

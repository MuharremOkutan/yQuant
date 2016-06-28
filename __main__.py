from yQuant.broker import Broker
import yQuant.logger
import yQuant.code
import yQuant.daily

def main():
    logger = yQuant.logger.get_logger()

    # 주식 코드 가져오기
    if yQuant.code.is_required_to_run(): yQuant.code.run()

    # 일자별 가격정보 가져오기
    yQuant.daily.run()

    # 방향성 판단
        #방향성: 거시경제 지표를 수집하고 가중평균으로 시장의 방향성을 표시
    # 백테스터: 전략모듈을 작동시켜 수집기가 준비한 내용으로 각 전략모듈의 수익성 체크

    broker = Broker()
    # 전략모듈: 거래시점 판단 및 주문실행 후 로깅
    # 로거: 전략모듈이 로깅한 주문 내역을 기록
    # 실행기: 자금관리 룰에 따라 각 전략모듈의 거래금액 한도를 설정하고, 모듈을 기동시킴
    # 평가: 로거의 주문 내역을 토대로 수수료와 세금을 감안하여 기간별 누적 수익률 계산


if __name__ == '__main__':
    main()

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import pyupbit
import pandas as pd
from notion_manager import NotionManager
from slack_notifier import SlackNotifier
from class_mrha import MRHATradingSystem

# .env 파일 로드
load_dotenv()

def get_account_balance():
    """업비트 계좌 잔고 조회"""
    try:
        access_key = os.getenv('UPBIT_ACCESS_KEY')
        secret_key = os.getenv('UPBIT_SECRET_KEY')
        upbit = pyupbit.Upbit(access_key, secret_key)
        return upbit.get_balances()
    except Exception as e:
        print(f"Error getting account balance: {e}")
        return []

def update_portfolio_db(notion_manager, balances):
    """포트폴리오 DB 업데이트"""
    try:
        portfolio_data = []
        
        # KRW 잔고 추가
        krw_balance = next((item for item in balances if item['currency'] == 'KRW'), None)
        if krw_balance:
            portfolio_data.append({
                'ticker': 'KRW',
                'amount': float(krw_balance['balance']),
                'avg_price': 1,
                'current_price': 1,
                'total_value': float(krw_balance['balance'])
            })
        
        # 코인 잔고 추가
        for balance in balances:
            if balance['currency'] != 'KRW':
                ticker = f"KRW-{balance['currency']}"
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    amount = float(balance['balance'])
                    avg_price = float(balance['avg_buy_price'])
                    total_value = amount * current_price
                    
                    portfolio_data.append({
                        'ticker': balance['currency'],
                        'amount': amount,
                        'avg_price': avg_price,
                        'current_price': current_price,
                        'total_value': total_value
                    })
        
        # Notion DB 업데이트
        notion_manager.update_portfolio(portfolio_data)
        print("포트폴리오 DB 업데이트 완료")
        
        return portfolio_data
    except Exception as e:
        print(f"Error updating portfolio DB: {e}")
        return []

def get_top_volume_coins(limit=10, owned_coins=None):
    """거래량 상위 코인 추출 (보유 코인 포함)"""
    try:
        # 모든 코인 정보 조회
        tickers = pyupbit.get_tickers(fiat="KRW")
        volumes = []
        
        # 보유 코인 목록
        owned_tickers = [f"KRW-{coin}" for coin in owned_coins] if owned_coins else []
        
        for ticker in tickers:
            try:
                # 24시간 거래량 정보 조회
                ticker_info = pyupbit.get_ohlcv(ticker, interval="day", count=1)
                if not ticker_info.empty:
                    volumes.append({
                        'ticker': ticker,
                        'trading_value': ticker_info['volume'].iloc[0] * ticker_info['close'].iloc[0],
                        'rank': 0,  # 임시값, 정렬 후 업데이트
                        'is_owned': ticker in owned_tickers
                    })
            except Exception as e:
                print(f"Error getting data for {ticker}: {e}")
                continue
        
        # 거래량 기준 정렬
        volumes.sort(key=lambda x: x['trading_value'], reverse=True)
        
        # 상위 limit개 선택
        top_coins = volumes[:limit]
        
        # 보유 코인이 상위 limit개에 없으면 추가
        for coin in volumes:
            if coin['is_owned'] and coin not in top_coins:
                top_coins.append(coin)
        
        # 순위 업데이트
        for i, volume in enumerate(top_coins, 1):
            volume['rank'] = i
        
        return top_coins
        
    except Exception as e:
        print(f"Error getting top volume coins: {e}")
        return []

def execute_trade(signal, notion_manager, upbit):
    """거래 실행"""
    try:
        ticker = signal['properties']['Ticker']['select']['name']
        signal_type = signal['properties']['Signal']['select']['name']
        full_ticker = f"KRW-{ticker}"
        signal_id = signal['id']  # 시그널 ID 저장
        
        if signal_type == 'SELL':
            # 보유 수량 확인
            balance = upbit.get_balance(full_ticker)
            if balance > 0:
                # 시장가 매도
                result = upbit.sell_market_order(full_ticker, balance)
                if result:
                    print(f"{ticker} {balance}개 시장가 매도 완료")
                    # 계좌 잔고 업데이트
                    balances = upbit.get_balances()
                    update_portfolio_db(notion_manager, balances)
                    # 시그널 상태 업데이트
                    notion_manager.update_signal_status(signal_id, "DONE")
                    print(f"{ticker} 시그널 상태 업데이트: DONE")
                else:
                    print(f"{ticker} 매도 실패")
            else:
                print(f"{ticker} 보유 수량 없음")
                notion_manager.update_signal_status(signal_id, "DONE")
                print(f"{ticker} 시그널 상태 업데이트: DONE (보유 수량 없음)")
        
        elif signal_type == 'BUY':
            # KRW 잔고 확인
            krw_balance = upbit.get_balance("KRW")
            if krw_balance >= 1000000:  # 100만원 이상
                # 시장가 매수
                result = upbit.buy_market_order(full_ticker, 1000000)
                if result:
                    print(f"{ticker} 100만원 시장가 매수 완료")
                    # 계좌 잔고 업데이트
                    balances = upbit.get_balances()
                    update_portfolio_db(notion_manager, balances)
                    # 시그널 상태 업데이트
                    notion_manager.update_signal_status(signal_id, "DONE")
                    print(f"{ticker} 시그널 상태 업데이트: DONE")
                else:
                    print(f"{ticker} 매수 실패")
            else:
                print(f"KRW 잔고 부족: {krw_balance}")
                notion_manager.update_signal_status(signal_id, "DONE")
                print(f"{ticker} 시그널 상태 업데이트: DONE (잔고 부족)")
        
        elif signal_type == 'HOLD':
            # HOLD 시그널은 바로 DONE으로 업데이트
            notion_manager.update_signal_status(signal_id, "DONE")
            print(f"{ticker} 시그널 상태 업데이트: DONE (HOLD)")
        
        return True
    except Exception as e:
        print(f"Error executing trade for {ticker}: {e}")
        return False

def verify_signal_execution(notion_manager):
    """시그널 실행 상태 확인"""
    try:
        # PENDING 시그널 조회
        pending_signals = notion_manager.get_pending_signals()
        if pending_signals:
            print("\n=== 미실행 시그널 확인 ===")
            for signal in pending_signals:
                ticker = signal['properties']['Ticker']['select']['name']
                signal_type = signal['properties']['Signal']['select']['name']
                print(f"{ticker}: {signal_type} 시그널이 아직 실행되지 않음")
            return False
        else:
            print("\n모든 시그널이 성공적으로 실행되었습니다.")
            return True
    except Exception as e:
        print(f"시그널 실행 상태 확인 중 오류 발생: {e}")
        return False

def get_current_balance():
    """현재 계좌 잔고 조회 및 정리"""
    try:
        balances = get_account_balance()
        total_balance = 0
        coins = {}
        
        # KRW 잔고 처리
        krw_balance = next((item for item in balances if item['currency'] == 'KRW'), None)
        if krw_balance:
            krw_amount = float(krw_balance['balance'])
            total_balance += krw_amount
            coins['KRW'] = krw_amount
        
        # 코인 잔고 처리
        for balance in balances:
            if balance['currency'] != 'KRW':
                ticker = f"KRW-{balance['currency']}"
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    amount = float(balance['balance'])
                    total_value = amount * current_price
                    total_balance += total_value
                    coins[balance['currency']] = amount
        
        return {
            'total_balance': total_balance,
            'coins': coins
        }
    except Exception as e:
        print(f"Error getting current balance: {e}")
        return {'total_balance': 0, 'coins': {}}

def get_portfolio_data(balance_info):
    """포트폴리오 데이터 생성"""
    try:
        portfolio_data = []
        
        # KRW 잔고 추가
        if 'KRW' in balance_info['coins']:
            portfolio_data.append({
                'ticker': 'KRW',
                'amount': balance_info['coins']['KRW'],
                'avg_price': 1,
                'current_price': 1,
                'total_value': balance_info['coins']['KRW']
            })
        
        # 코인 잔고 추가
        for coin, amount in balance_info['coins'].items():
            if coin != 'KRW':
                ticker = f"KRW-{coin}"
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    # 평균 매수가 조회
                    upbit = pyupbit.Upbit(os.getenv('UPBIT_ACCESS_KEY'), os.getenv('UPBIT_SECRET_KEY'))
                    balance = upbit.get_balance(ticker)
                    avg_price = float(balance['avg_buy_price']) if balance else current_price
                    total_value = amount * current_price
                    
                    portfolio_data.append({
                        'ticker': coin,
                        'amount': amount,
                        'avg_price': avg_price,
                        'current_price': current_price,
                        'total_value': total_value
                    })
        
        return portfolio_data
    except Exception as e:
        print(f"Error getting portfolio data: {e}")
        return []

def wait_until_signal_generation_time():
    """시그널 생성 시간(09:01:00)까지 대기"""
    now = datetime.now()
    signal_time = now.replace(hour=9, minute=1, second=0, microsecond=0)
    
    if now >= signal_time:
        signal_time = signal_time.replace(day=signal_time.day + 1)
    
    wait_seconds = (signal_time - now).total_seconds()
    print(f"시그널 생성까지 {wait_seconds/3600:.1f}시간 대기 중...")
    time.sleep(wait_seconds)

def wait_until_execution_time():
    """시그널 실행 시간(09:05:00)까지 대기"""
    now = datetime.now()
    execution_time = now.replace(hour=9, minute=5, second=0, microsecond=0)
    
    if now >= execution_time:
        execution_time = execution_time.replace(day=execution_time.day + 1)
    
    wait_seconds = (execution_time - now).total_seconds()
    print(f"시그널 실행까지 {wait_seconds/3600:.1f}시간 대기 중...")
    time.sleep(wait_seconds)

def run_trading_system():
    # 시스템 초기화
    notion_manager = NotionManager()
    slack = SlackNotifier()
    access_key = os.getenv('UPBIT_ACCESS_KEY')
    secret_key = os.getenv('UPBIT_SECRET_KEY')
    upbit = pyupbit.Upbit(access_key, secret_key)
    
    # 시작 알림 (에러 처리 추가)
    try:
        print("Slack 메시지 전송 시도...")
        result = slack.send_notification(f"""
🚀 MRHA 트레이딩 시스템 시작
시작시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
상태: 정상 작동 중
""")
        print(f"Slack 메시지 전송 결과: {'성공' if result else '실패'}")
    except Exception as e:
        print(f"Slack 메시지 전송 중 에러 발생: {e}")
    
    try:
        # 1. 계좌 잔고 조회 및 포트폴리오 DB 업데이트
        print("\n=== 계좌 잔고 조회 및 포트폴리오 업데이트 ===")
        balances = get_account_balance()
        portfolio_data = update_portfolio_db(notion_manager, balances)
        
        # 보유 중인 코인 목록 추출
        owned_coins = [item['ticker'] for item in portfolio_data if item['ticker'] != 'KRW']
        print(f"보유 중인 코인: {owned_coins}")
        
        # Slack 알림: 포트폴리오 업데이트
        slack.send_notification(f"""
📊 포트폴리오 업데이트 완료
보유 코인: {', '.join(owned_coins) if owned_coins else '없음'}
KRW 잔고: {next((item['amount'] for item in portfolio_data if item['ticker'] == 'KRW'), 0):,.0f}원
""")
        
        # 2. Top 10 코인 선별 (거래량 기준 + 보유 코인)
        print("\n=== Top 10 코인 선별 ===")
        top_coins = get_top_volume_coins(limit=10, owned_coins=owned_coins)
        print(f"선별된 코인 수: {len(top_coins)}개")
        
        # Slack 알림: 선정된 코인
        selected_coins = [coin['ticker'].replace('KRW-', '') for coin in top_coins]
        slack.send_notification(f"""
🎯 선정된 코인 목록
총 {len(selected_coins)}개: {', '.join(selected_coins)}
""")
        
        # 3. MRHA 시그널 생성
        print("\n=== MRHA 시그널 생성 ===")
        signals = []
        signal_summary = {'BUY': [], 'SELL': [], 'HOLD': []}
        
        # 전일 날짜 계산
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        for coin in top_coins:
            try:
                # MRHA 분석 실행 (전일 일봉 포함 365일 데이터)
                bot = MRHATradingSystem(coin['ticker'], "day", count=365)
                bot.run_analysis()
                
                # 전일 시그널 확인
                last_signal = "HOLD"
                for _, trade in bot.trades.iterrows():
                    trade_date = trade['Date'].strftime("%Y-%m-%d")
                    if trade_date == yesterday:
                        last_signal = "BUY" if trade['Type'] == 'Buy' else "SELL"
                        break
                
                signals.append({
                    'ticker': coin['ticker'].replace('KRW-', ''),
                    'rank': coin['rank'],
                    'trading_value': coin['trading_value'],
                    'signal': last_signal,
                    'status': 'PENDING'
                })
                signal_summary[last_signal].append(coin['ticker'].replace('KRW-', ''))
                print(f"{coin['ticker']}: {last_signal} 시그널 생성")
            
            except Exception as e:
                print(f"Error processing {coin['ticker']}: {e}")
                continue
        
        # Slack 알림: 시그널 생성 결과
        slack.send_notification(f"""
📈 MRHA 시그널 생성 완료
BUY: {', '.join(signal_summary['BUY']) if signal_summary['BUY'] else '없음'}
SELL: {', '.join(signal_summary['SELL']) if signal_summary['SELL'] else '없음'}
HOLD: {', '.join(signal_summary['HOLD']) if signal_summary['HOLD'] else '없음'}
""")
        
        # 4. Notion DB 업데이트
        notion_manager.update_daily_signals(signals)
        print("시그널 DB 업데이트 완료")
        
        # 5. 시그널 실행 시간까지 대기
        print("\n시그널 실행 시간까지 대기 중...")
        slack.send_notification("⏳ 시그널 실행 시간까지 대기 중...")
        wait_until_execution_time()
        
        # 6. 계좌 정보 재조회
        print("\n=== 계좌 정보 재조회 ===")
        balances = get_account_balance()
        portfolio_data = update_portfolio_db(notion_manager, balances)
        
        # 7. PENDING 시그널 실행
        print("\n=== 시그널 실행 시작 ===")
        slack.send_notification("🔄 시그널 실행 시작")
        
        # PENDING 시그널 조회
        pending_signals = notion_manager.get_pending_signals()
        print(f"PENDING 시그널 수: {len(pending_signals)}")
        
        # SELL 시그널 먼저 실행
        sell_signals = [s for s in pending_signals if s['properties']['Signal']['select']['name'] == 'SELL']
        if sell_signals:
            slack.send_notification("💰 SELL 시그널 실행 시작")
            for signal in sell_signals:
                execute_trade(signal, notion_manager, upbit)
        
        # BUY 시그널 실행
        buy_signals = [s for s in pending_signals if s['properties']['Signal']['select']['name'] == 'BUY']
        if buy_signals:
            slack.send_notification("💎 BUY 시그널 실행 시작")
            for signal in buy_signals:
                execute_trade(signal, notion_manager, upbit)
        
        # HOLD 시그널 실행
        hold_signals = [s for s in pending_signals if s['properties']['Signal']['select']['name'] == 'HOLD']
        if hold_signals:
            slack.send_notification("⏸️ HOLD 시그널 처리 시작")
            for signal in hold_signals:
                execute_trade(signal, notion_manager, upbit)
        
        # 시그널 실행 상태 확인
        execution_status = verify_signal_execution(notion_manager)
        
        # 최종 포트폴리오 상태 조회
        final_balances = get_account_balance()
        final_portfolio = update_portfolio_db(notion_manager, final_balances)
        
        # Slack 알림: 작업 완료
        slack.send_notification(f"""
✅ 트레이딩 시스템 작업 완료
시그널 실행 상태: {'성공' if execution_status else '일부 미실행'}
최종 KRW 잔고: {next((item['amount'] for item in final_portfolio if item['ticker'] == 'KRW'), 0):,.0f}원
보유 코인: {', '.join([item['ticker'] for item in final_portfolio if item['ticker'] != 'KRW']) if any(item['ticker'] != 'KRW' for item in final_portfolio) else '없음'}
""")
        
        print("\n=== 작업 완료 ===")
        return True
        
    except Exception as e:
        error_message = f"작업 중 오류 발생: {e}"
        print(error_message)
        slack.send_notification(f"❌ {error_message}")
        return False

if __name__ == "__main__":
    while True:
        try:
            # 시그널 생성 시간까지 대기
            wait_until_signal_generation_time()
            # 트레이딩 시스템 실행
            run_trading_system()
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(60)  # 오류 발생 시 1분 대기 후 재시도 

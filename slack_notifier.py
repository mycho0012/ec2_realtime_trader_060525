import os
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime

# .env 파일 로드
load_dotenv()

class SlackNotifier:
    def __init__(self):
        self.client = WebClient(token=os.getenv('SLACK_BOT_TOKEN'))
        self.channel = os.getenv('SLACK_CHANNEL')
        print(f"SlackNotifier 초기화: 채널={self.channel}")
        
    def send_notification(self, message):
        """기본 Slack 메시지 전송"""
        try:
            print(f"Slack 메시지 전송 시도: {message[:50]}...")
            response = self.client.chat_postMessage(
                channel=self.channel,
                text=message
            )
            if response["ok"]:
                print("Slack 메시지 전송 성공!")
                return True
            else:
                print(f"Slack 메시지 전송 실패: {response.get('error', '알 수 없는 에러')}")
                return False
        except SlackApiError as e:
            print(f"Slack API 에러: {e.response['error']}")
            return False
        except Exception as e:
            print(f"Slack 메시지 전송 중 예상치 못한 에러: {str(e)}")
            return False
            
    def notify_signal_execution(self, execution_type, data):
        """시그널 실행 결과 알림"""
        try:
            if execution_type == "SELL":
                message = self._format_sell_notification(data)
            elif execution_type == "BUY":
                message = self._format_buy_notification(data)
            elif execution_type == "HOLD":
                message = self._format_hold_notification(data)
            else:
                return False
                
            return self.send_notification(message)
        except Exception as e:
            print(f"Error sending execution notification: {e}")
            return False
            
    def _format_sell_notification(self, data):
        """매도 실행 알림 포맷"""
        return f"""
🔴 매도 실행 완료
코인: {data['ticker']}
수량: {data['amount']}
평균가: {data['avg_price']:,.0f} KRW
실행가: {data['execution_price']:,.0f} KRW
수익률: {data['profit_rate']:.2f}%
실행시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            
    def _format_buy_notification(self, data):
        """매수 실행 알림 포맷"""
        return f"""
🟢 매수 실행 완료
코인: {data['ticker']}
수량: {data['amount']}
매수가: {data['execution_price']:,.0f} KRW
투자금액: {data['investment_amount']:,.0f} KRW
실행시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            
    def _format_hold_notification(self, data):
        """홀드 상태 알림 포맷"""
        return f"""
⚪ 홀드 상태 유지
코인: {data['ticker']}
보유수량: {data['amount']}
평균가: {data['avg_price']:,.0f} KRW
현재가: {data['current_price']:,.0f} KRW
수익률: {data['profit_rate']:.2f}%
업데이트시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            
    def notify_error(self, error_type, error_message):
        """에러 알림"""
        message = f"""
❌ 에러 발생
유형: {error_type}
메시지: {error_message}
발생시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        return self.send_notification(message) 

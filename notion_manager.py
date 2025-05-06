from notion_client import Client
import os
from dotenv import load_dotenv
from datetime import datetime
import time
from slack_notifier import SlackNotifier

# .env 파일 로드
load_dotenv()

class NotionManager:
    def __init__(self):
        # 환경 변수 로드 확인
        notion_token = os.getenv('NOTION_TOKEN')
        daily_signals_db_id = os.getenv('DAILY_SIGNALS_DB_ID')
        portfolio_db_id = os.getenv('PORTFOLIO_DB_ID')
        
        print(f"Environment variables loaded:")
        print(f"NOTION_TOKEN: {'Set' if notion_token else 'Not set'}")
        print(f"DAILY_SIGNALS_DB_ID: {'Set' if daily_signals_db_id else 'Not set'}")
        print(f"PORTFOLIO_DB_ID: {'Set' if portfolio_db_id else 'Not set'}")
        
        if not all([notion_token, daily_signals_db_id, portfolio_db_id]):
            raise ValueError("Required environment variables are not set")
            
        self.notion = Client(auth=notion_token)
        self.daily_signals_db_id = daily_signals_db_id
        self.portfolio_db_id = portfolio_db_id
        self.slack = SlackNotifier()
        
    def update_daily_signals(self, signals_data):
        """00:00 작업 - Daily Signals DB 업데이트"""
        try:
            print(f"\n=== Daily Signals DB 업데이트 시작 ===")
            print(f"데이터베이스 ID: {self.daily_signals_db_id}")
            print(f"시그널 데이터 수: {len(signals_data)}")
            
            # 기존 데이터 삭제
            print("기존 데이터 삭제 시도...")
            self._clear_signals_db()
            print("기존 데이터 삭제 완료")
            
            # 새로운 시그널 데이터 추가
            for signal in signals_data:
                print(f"\n시그널 추가 시도: {signal['ticker']}")
                try:
                    self.notion.pages.create(
                        parent={"database_id": self.daily_signals_db_id},
                        properties={
                            "Record ID": {
                                "title": [{
                                    "text": {
                                        "content": f"{datetime.now().strftime('%Y%m%d')}-{signal['ticker']}"
                                    }
                                }]
                            },
                            "Date": {
                                "date": {
                                    "start": datetime.now().strftime('%Y-%m-%d')
                                }
                            },
                            "Ticker": {
                                "select": {
                                    "name": signal['ticker']
                                }
                            },
                            "Rank": {
                                "number": signal['rank']
                            },
                            "Trading_Value": {
                                "number": signal['trading_value']
                            },
                            "Signal": {
                                "select": {
                                    "name": signal['signal']
                                }
                            },
                            "Status": {
                                "select": {
                                    "name": "PENDING"
                                }
                            },
                            "Execution_time": {
                                "date": {
                                    "start": datetime.now().strftime('%Y-%m-%d')
                                }
                            },
                            "Error_Message": {
                                "rich_text": [{
                                    "text": {
                                        "content": ""
                                    }
                                }]
                            },
                            "Retry_Count": {
                                "number": 0
                            }
                        }
                    )
                    print(f"{signal['ticker']} 시그널 추가 성공")
                except Exception as e:
                    print(f"{signal['ticker']} 시그널 추가 실패: {e}")
                    raise
            
            # 시그널 생성 알림
            self.slack.send_notification(f"""
📊 일일 시그널 생성 완료
생성시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
총 시그널 수: {len(signals_data)}
BUY 시그널: {len([s for s in signals_data if s['signal'] == 'BUY'])}
SELL 시그널: {len([s for s in signals_data if s['signal'] == 'SELL'])}
HOLD 시그널: {len([s for s in signals_data if s['signal'] == 'HOLD'])}
""")
            
            print("\n=== Daily Signals DB 업데이트 완료 ===")
            return True
        except Exception as e:
            error_msg = f"Error updating signals: {e}"
            print(f"\n에러 발생: {error_msg}")
            self.slack.notify_error("시그널 업데이트 실패", error_msg)
            return False

    def update_portfolio(self, portfolio_data):
        """포트폴리오 DB 업데이트"""
        try:
            print("\n=== 포트폴리오 DB 업데이트 시작 ===")
            
            # 기존 데이터 삭제
            print("기존 포트폴리오 데이터 삭제 시도...")
            results = self.notion.databases.query(
                database_id=self.portfolio_db_id
            )
            for page in results['results']:
                self.notion.pages.update(
                    page_id=page['id'],
                    archived=True
                )
            print("기존 포트폴리오 데이터 삭제 완료")
            
            # 새로운 포트폴리오 데이터 추가
            for position in portfolio_data:
                try:
                    self.notion.pages.create(
                        parent={"database_id": self.portfolio_db_id},
                        properties={
                            "Position ID": {
                                "title": [{
                                    "text": {
                                        "content": position['ticker']
                                    }
                                }]
                            },
                            "Ticker": {
                                "select": {
                                    "name": position['ticker']
                                }
                            },
                            "Amount": {
                                "number": position['amount']
                            },
                            "Average_Price": {
                                "number": position['avg_price']
                            },
                            "Current_Price": {
                                "number": position['current_price']
                            },
                            "Total_Value": {
                                "number": position['total_value']
                            },
                            "Last_Update": {
                                "date": {
                                    "start": datetime.now().isoformat()
                                }
                            }
                        }
                    )
                    print(f"{position['ticker']} 포지션 추가 성공")
                except Exception as e:
                    print(f"{position['ticker']} 포지션 추가 실패: {e}")
                    raise
            
            # 포트폴리오 업데이트 알림
            self.slack.send_notification(f"""
💼 포트폴리오 업데이트 완료
업데이트시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
총 자산: {sum(p['total_value'] for p in portfolio_data):,.0f} KRW
보유 코인 수: {len(portfolio_data) - 1}  # KRW 제외
""")
            
            print("=== 포트폴리오 DB 업데이트 완료 ===")
            return True
        except Exception as e:
            error_msg = f"Error updating portfolio: {e}"
            self.slack.notify_error("포트폴리오 업데이트 실패", error_msg)
            return False

    def update_signal_status(self, signal_id, status, execution_data=None):
        """시그널 상태 업데이트"""
        try:
            # 시그널 ID로 직접 업데이트
            self.notion.pages.update(
                page_id=signal_id,
                properties={
                    "Status": {
                        "select": {
                            "name": status
                        }
                    },
                    "Execution_time": {
                        "date": {
                            "start": datetime.now().isoformat()
                        }
                    }
                }
            )
            return True
        except Exception as e:
            error_msg = f"Error updating signal status: {e}"
            print(error_msg)
            self.slack.notify_error("시그널 상태 업데이트 실패", error_msg)
            return False

    def _clear_signals_db(self):
        """시그널 DB 초기화"""
        try:
            results = self.notion.databases.query(
                database_id=self.daily_signals_db_id
            )
            for page in results['results']:
                self.notion.pages.update(
                    page_id=page['id'],
                    archived=True
                )
            return True
        except Exception as e:
            error_msg = f"Error clearing signals DB: {e}"
            self.slack.notify_error("시그널 DB 초기화 실패", error_msg)
            return False

    def get_pending_signals(self):
        """PENDING 상태의 시그널 조회"""
        try:
            results = self.notion.databases.query(
                database_id=self.daily_signals_db_id,
                filter={
                    "property": "Status",
                    "select": {
                        "equals": "PENDING"
                    }
                }
            )
            return results['results']
        except Exception as e:
            error_msg = f"Error getting pending signals: {e}"
            self.slack.notify_error("시그널 조회 실패", error_msg)
            return []

    def get_current_portfolio(self):
        """현재 포트폴리오 조회"""
        try:
            results = self.notion.databases.query(
                database_id=self.portfolio_db_id
            )
            return results['results']
        except Exception as e:
            error_msg = f"Error getting portfolio: {e}"
            self.slack.notify_error("포트폴리오 조회 실패", error_msg)
            return [] 
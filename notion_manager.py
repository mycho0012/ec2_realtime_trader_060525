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
        self.api_call_delay = 0.35 # API 호출 간 지연 시간 (초)

    def _clear_database(self, database_id, db_name=""):
        """지정된 데이터베이스의 모든 페이지를 보관 처리 (페이징 처리 및 API 지연 포함)"""
        print(f"기존 {db_name} 데이터 삭제(보관) 시도...")
        all_pages_archived_successfully = True
        archived_count = 0
        try:
            has_more = True
            start_cursor = None
            while has_more:
                results = self.notion.databases.query(
                    database_id=database_id,
                    start_cursor=start_cursor
                )
                time.sleep(self.api_call_delay) # databases.query 호출 후 지연

                for page in results['results']:
                    try:
                        self.notion.pages.update(
                            page_id=page['id'],
                            archived=True
                        )
                        archived_count += 1
                        print(f"페이지 보관 처리 성공 (ID: {page['id']})")
                        time.sleep(self.api_call_delay) # pages.update 호출 후 지연
                    except Exception as page_e:
                        print(f"페이지 보관 처리 실패 (ID: {page['id']}): {page_e}")
                        all_pages_archived_successfully = False
                        # 개별 페이지 실패 시에도 계속 진행하도록 하거나, 여기서 raise하여 중단할 수 있음
                
                has_more = results.get('has_more', False) # get으로 안전하게 접근
                start_cursor = results.get('next_cursor') # get으로 안전하게 접근
            
            if all_pages_archived_successfully:
                print(f"기존 {db_name} 데이터 {archived_count}개 보관 처리 완료.")
            else:
                print(f"기존 {db_name} 데이터 {archived_count}개 보관 처리 중 일부 실패 발생.")
            return all_pages_archived_successfully
        except Exception as e:
            error_msg = f"Error clearing {db_name} DB: {e}"
            print(error_msg)
            self.slack.notify_error(f"{db_name} DB 초기화 실패", error_msg)
            return False

    def update_daily_signals(self, signals_data):
        """00:00 작업 - Daily Signals DB 업데이트"""
        try:
            print(f"\n=== Daily Signals DB 업데이트 시작 ===")
            print(f"데이터베이스 ID: {self.daily_signals_db_id}")
            print(f"시그널 데이터 수: {len(signals_data)}")
            
            # 기존 데이터 삭제 (페이징 처리 및 지연 포함)
            if not self._clear_database(self.daily_signals_db_id, "Daily Signals"):
                print("시그널 DB 초기화에 실패하여 시그널 업데이트를 중단합니다.")
                # 필요시 더 강력한 알림 또는 에러 처리
                return False
            
            # 새로운 시그널 데이터 추가
            successful_adds = 0
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
                            "Execution_time": { # 생성 시 Execution_time은 비워두거나, 예상 시간으로 설정. 실제 실행 후 업데이트.
                                "date": None # 또는 특정 값으로 초기화
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
                    successful_adds +=1
                    time.sleep(self.api_call_delay) # pages.create 호출 후 지연
                except Exception as e:
                    print(f"{signal['ticker']} 시그널 추가 실패: {e}")
                    # 개별 시그널 추가 실패 시 전체를 중단할지(raise) 또는 계속 진행할지 결정
                    # 여기서는 에러를 출력하고 다음 시그널로 넘어감
                    self.slack.notify_error(f"{signal['ticker']} 시그널 추가 실패", str(e))
            
            print(f"\n총 {len(signals_data)}개 중 {successful_adds}개 시그널 추가 완료.")
            # 시그널 생성 알림
            self.slack.send_notification(f"""
📊 일일 시그널 생성 완료
생성시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
요청 시그널 수: {len(signals_data)}
성공 시그널 수: {successful_adds}
BUY 시그널: {len([s for s in signals_data if s['signal'] == 'BUY' and s in signals_data[:successful_adds]])} # 성공한 것들 중에서 카운트
SELL 시그널: {len([s for s in signals_data if s['signal'] == 'SELL' and s in signals_data[:successful_adds]])}
HOLD 시그널: {len([s for s in signals_data if s['signal'] == 'HOLD' and s in signals_data[:successful_adds]])}
""")
            
            print("\n=== Daily Signals DB 업데이트 완료 ===")
            return successful_adds == len(signals_data) # 모든 시그널이 성공적으로 추가되었는지 여부 반환
        except Exception as e:
            error_msg = f"Error updating signals: {e}"
            print(f"\n에러 발생: {error_msg}")
            self.slack.notify_error("시그널 업데이트 중 심각한 오류 발생", error_msg)
            return False

    def update_portfolio(self, portfolio_data):
        """포트폴리오 DB 업데이트"""
        try:
            print("\n=== 포트폴리오 DB 업데이트 시작 ===")
            
            # 기존 데이터 삭제 (페이징 처리 및 지연 포함)
            if not self._clear_database(self.portfolio_db_id, "Portfolio"):
                print("포트폴리오 DB 초기화에 실패하여 업데이트를 중단합니다.")
                return False
            
            # 새로운 포트폴리오 데이터 추가
            successful_adds = 0
            for position in portfolio_data:
                print(f"\n포지션 추가 시도: {position['ticker']}")
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
                    successful_adds += 1
                    time.sleep(self.api_call_delay) # pages.create 호출 후 지연
                except Exception as e:
                    print(f"{position['ticker']} 포지션 추가 실패: {e}")
                    self.slack.notify_error(f"{position['ticker']} 포지션 추가 실패", str(e))

            print(f"\n총 {len(portfolio_data)}개 중 {successful_adds}개 포지션 추가 완료.")
            # 포트폴리오 업데이트 알림
            self.slack.send_notification(f"""
💼 포트폴리오 업데이트 완료
업데이트시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
총 자산: {sum(p['total_value'] for p in portfolio_data):,.0f} KRW
보유 코인 수 (KRW 제외): {len([p for p in portfolio_data if p['ticker'] != 'KRW' and p in portfolio_data[:successful_adds]])}
""")
            
            print("=== 포트폴리오 DB 업데이트 완료 ===")
            return successful_adds == len(portfolio_data)
        except Exception as e:
            error_msg = f"Error updating portfolio: {e}"
            print(f"\n에러 발생: {error_msg}")
            self.slack.notify_error("포트폴리오 업데이트 중 심각한 오류 발생", error_msg)
            return False

    def update_signal_status(self, signal_id, status, execution_data=None): # execution_data는 현재 사용 안됨
        """시그널 상태 업데이트"""
        try:
            print(f"시그널 상태 업데이트 시도 (ID: {signal_id}, Status: {status})")
            properties_to_update = {
                "Status": {
                    "select": {
                        "name": status
                    }
                },
                "Execution_time": { # 시그널 실행 시 실제 실행 시간으로 업데이트
                    "date": {
                        "start": datetime.now().isoformat()
                    }
                }
            }
            # 필요하다면 execution_data를 사용하여 다른 필드도 업데이트
            # 예: if execution_data and 'error_message' in execution_data:
            #         properties_to_update["Error_Message"] = {"rich_text": [{"text": {"content": execution_data['error_message']}}]}

            self.notion.pages.update(
                page_id=signal_id,
                properties=properties_to_update
            )
            print(f"시그널 상태 업데이트 성공 (ID: {signal_id})")
            time.sleep(self.api_call_delay) # pages.update 호출 후 지연
            return True
        except Exception as e:
            error_msg = f"Error updating signal status (ID: {signal_id}): {e}"
            print(error_msg)
            self.slack.notify_error(f"시그널 상태 업데이트 실패 (ID: {signal_id})", error_msg)
            return False

    def _clear_signals_db(self):
        """
        이 메소드는 _clear_database로 대체되었습니다.
        호출하는 곳이 있다면 _clear_database(self.daily_signals_db_id, "Daily Signals")로 변경해야 합니다.
        일단 이전 호출과의 호환성을 위해 남겨두지만, 사용하지 않는 것을 권장합니다.
        """
        print("경고: _clear_signals_db()는 _clear_database()로 대체되었습니다. 코드 수정을 권장합니다.")
        return self._clear_database(self.daily_signals_db_id, "Daily Signals (deprecated call)")


    def get_pending_signals(self):
        """PENDING 상태의 시그널 조회 (페이징 처리 및 API 지연 포함)"""
        print("PENDING 시그널 조회 시도...")
        pending_signals = []
        try:
            has_more = True
            start_cursor = None
            while has_more:
                results = self.notion.databases.query(
                    database_id=self.daily_signals_db_id,
                    filter={
                        "property": "Status",
                        "select": {
                            "equals": "PENDING"
                        }
                    },
                    start_cursor=start_cursor
                )
                time.sleep(self.api_call_delay) # databases.query 호출 후 지연

                pending_signals.extend(results['results'])
                
                has_more = results.get('has_more', False)
                start_cursor = results.get('next_cursor')
            
            print(f"PENDING 시그널 {len(pending_signals)}개 조회 완료.")
            return pending_signals
        except Exception as e:
            error_msg = f"Error getting pending signals: {e}"
            print(error_msg)
            self.slack.notify_error("PENDING 시그널 조회 실패", error_msg)
            return []

    def get_current_portfolio(self):
        """현재 포트폴리오 조회 (페이징 처리 및 API 지연 포함)"""
        print("현재 포트폴리오 조회 시도...")
        portfolio_items = []
        try:
            has_more = True
            start_cursor = None
            while has_more:
                results = self.notion.databases.query(
                    database_id=self.portfolio_db_id,
                    start_cursor=start_cursor
                    # 필터가 필요하다면 여기에 추가
                )
                time.sleep(self.api_call_delay) # databases.query 호출 후 지연
                
                portfolio_items.extend(results['results'])

                has_more = results.get('has_more', False)
                start_cursor = results.get('next_cursor')

            print(f"포트폴리오 항목 {len(portfolio_items)}개 조회 완료.")
            return portfolio_items
        except Exception as e:
            error_msg = f"Error getting portfolio: {e}"
            print(error_msg)
            self.slack.notify_error("포트폴리오 조회 실패", error_msg)
            return []

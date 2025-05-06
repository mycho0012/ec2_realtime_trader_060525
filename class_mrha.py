import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pyupbit


class MRHATradingSystem:

    def __init__(self, symbol, interval, count):
        self.symbol = symbol
        self.interval = interval
        self.count = count
        self.stock_data = None
        self.mrha_data = None
        self.backtest_results = None
        self.trades = None

    def download_data(self):
        df = pyupbit.get_ohlcv(self.symbol, interval=self.interval, count=self.count)
        df = df.rename(columns=lambda x: x.capitalize())
        df = df.drop(columns='Value')
        df.index.name = 'Date'
        self.stock_data = df
        return self.stock_data

    def calculate_revised_heikin_ashi(self):
        if self.stock_data.index.duplicated().any():
            raise ValueError("Duplicate dates found in stock_data index. Please check the data.")
    
        ha = self.stock_data[['Open', 'High', 'Low', 'Close']].copy()
        ha.columns = ['h_open', 'h_high', 'h_low', 'h_close']
        ha['h_close'] = (ha['h_open'] + ha['h_high'] + ha['h_low'] + ha['h_close']) / 4
        for i in range(1, len(ha)):
            ha.iloc[i, 0] = (ha.iloc[i-1, 0] + ha.iloc[i-1, 3]) / 2
        ha['h_high'] = ha[['h_open', 'h_close']].join(self.stock_data['High']).max(axis=1)
        ha['h_low'] = ha[['h_open', 'h_close']].join(self.stock_data['Low']).min(axis=1)
        return ha

    def calculate_mrha(self, rha_data):
        mrha = pd.DataFrame(index=self.stock_data.index, columns=['mh_open', 'mh_high', 'mh_low', 'mh_close'])
        mrha['mh_open'] = (rha_data['h_open'] + rha_data['h_close']) / 2
        mrha['mh_high'] = rha_data['h_open'].rolling(window=5).mean()
        mrha['mh_low'] = rha_data['h_low'].rolling(window=5).mean()
        mrha['mh_close'] = (mrha['mh_open'] + self.stock_data['High'] + self.stock_data['Low'] + self.stock_data['Close'] * 2) / 5
        return mrha.dropna()

    def add_trading_signals(self):
        def calculate_ebr(mh_open, low):
            return (4 * mh_open - low) / 3

        def calculate_btrg(ebr):
            return 1.00618 * ebr

        def calculate_ebl(mh_open, high):
            return (4 * mh_open - high) / 3

        def calculate_strg(ebl):
            return 0.99382 * ebl

        signals = pd.DataFrame(index=self.mrha_data.index, columns=['Ebr', 'Btrg', 'Ebl', 'Strg'])
        signals['Ebr'] = calculate_ebr(self.mrha_data['mh_open'], self.stock_data['Low'])
        signals['Btrg'] = calculate_btrg(signals['Ebr'])
        signals['Ebl'] = calculate_ebl(self.mrha_data['mh_open'], self.stock_data['High'])
        signals['Strg'] = calculate_strg(signals['Ebl'])
        
        self.mrha_data = pd.concat([self.mrha_data, signals], axis=1)

    def calculate_price_targets(self):
        targets = pd.DataFrame(index=self.stock_data.index, columns=['Bullish_Target', 'Bearish_Target'])
        high_5d = self.stock_data['High'].rolling(window=5).max()
        low_5d = self.stock_data['Low'].rolling(window=5).min()
        targets['Bullish_Target'] = low_5d * 1.0618
        targets['Bearish_Target'] = high_5d * 0.9382
        self.mrha_data = pd.concat([self.mrha_data, targets], axis=1)

    def calculate_td_setup(self):
        self.mrha_data['Close_4_bars_ago'] = self.mrha_data['mh_close'].shift(4)
        self.mrha_data['TD_Buy_Setup'] = 0
        self.mrha_data['TD_Sell_Setup'] = 0
        buy_count = 0
        sell_count = 0

        for i in range(len(self.mrha_data)):
            if self.mrha_data['mh_close'].iloc[i] < self.mrha_data['Close_4_bars_ago'].iloc[i]:
                buy_count += 1
                sell_count = 0
            elif self.mrha_data['mh_close'].iloc[i] > self.mrha_data['Close_4_bars_ago'].iloc[i]:
                sell_count += 1
                buy_count = 0
            else:
                buy_count = 0
                sell_count = 0

            if buy_count == 9:
                self.mrha_data.loc[self.mrha_data.index[i-8:i+1], 'TD_Buy_Setup'] = range(1, 10)
                buy_count = 0
            if sell_count == 9:
                self.mrha_data.loc[self.mrha_data.index[i-8:i+1], 'TD_Sell_Setup'] = range(1, 10)
                sell_count = 0

    def implement_trading_logic(self):
        signals = pd.DataFrame(index=self.mrha_data.index, columns=['Signal', 'Position', 'Entry_Price', 'Exit_Price'])
        position = 0
        
        for i in range(1, len(self.mrha_data)):
            bullish_candle = self.mrha_data['mh_close'].iloc[i] > self.mrha_data['mh_open'].iloc[i] and \
                             self.mrha_data['mh_close'].iloc[i] > self.mrha_data['mh_high'].iloc[i-1]
            
            bearish_candle = self.mrha_data['mh_close'].iloc[i] < self.mrha_data['mh_open'].iloc[i] and \
                             self.mrha_data['mh_close'].iloc[i] < self.mrha_data['mh_low'].iloc[i-1]
            
            if position == 0 and bullish_candle and self.mrha_data['mh_close'].iloc[i] > self.mrha_data['Btrg'].iloc[i]:
                signals.loc[self.mrha_data.index[i], 'Signal'] = 1
                signals.loc[self.mrha_data.index[i], 'Entry_Price'] = self.mrha_data['mh_close'].iloc[i]
                position = 1
            elif position == 0 and bearish_candle and self.mrha_data['mh_close'].iloc[i] < self.mrha_data['Strg'].iloc[i]:
                signals.loc[self.mrha_data.index[i], 'Signal'] = -1
                signals.loc[self.mrha_data.index[i], 'Entry_Price'] = self.mrha_data['mh_close'].iloc[i]
                position = -1
            elif position == 1 and (self.mrha_data['mh_close'].iloc[i] < self.mrha_data['Ebl'].iloc[i] or self.mrha_data['mh_close'].iloc[i] > self.mrha_data['Bullish_Target'].iloc[i]):
                signals.loc[self.mrha_data.index[i], 'Signal'] = 0
                signals.loc[self.mrha_data.index[i], 'Exit_Price'] = self.mrha_data['mh_close'].iloc[i]
                position = 0
            elif position == -1 and (self.mrha_data['mh_close'].iloc[i] > self.mrha_data['Ebr'].iloc[i] or self.mrha_data['mh_close'].iloc[i] < self.mrha_data['Bearish_Target'].iloc[i]):
                signals.loc[self.mrha_data.index[i], 'Signal'] = 0
                signals.loc[self.mrha_data.index[i], 'Exit_Price'] = self.mrha_data['mh_close'].iloc[i]
                position = 0
            
            signals.loc[self.mrha_data.index[i], 'Position'] = position
        
        self.mrha_data = pd.concat([self.mrha_data, signals], axis=1)

    def run_backtest(self, initial_capital=100000000, commission=0.001):
        portfolio = pd.DataFrame(index=self.mrha_data.index, columns=['Holdings', 'Cash', 'Total_Value', 'Returns'])
        portfolio.loc[portfolio.index[0], 'Holdings'] = 0
        portfolio.loc[portfolio.index[0], 'Cash'] = initial_capital
        portfolio.loc[portfolio.index[0], 'Total_Value'] = initial_capital
        portfolio.loc[portfolio.index[0], 'Returns'] = 0
        
        trades = []
        position = 0
        
        for i in range(1, len(self.mrha_data)):
            current_price = self.mrha_data['mh_close'].iloc[i]
            signal = self.mrha_data['Signal'].iloc[i]
            
            if signal == 1 and position == 0:
                shares_to_buy = portfolio.loc[portfolio.index[i-1], 'Cash'] // (current_price * (1 + commission))
                cost = shares_to_buy * current_price * (1 + commission)
                portfolio.loc[portfolio.index[i], 'Holdings'] = shares_to_buy
                portfolio.loc[portfolio.index[i], 'Cash'] = portfolio.loc[portfolio.index[i-1], 'Cash'] - cost
                position = 1
                trades.append({'Date': self.mrha_data.index[i], 'Type': 'Buy', 'Price': current_price, 'Shares': shares_to_buy})
            elif signal == -1 and position == 1:
                shares_to_sell = portfolio.loc[portfolio.index[i-1], 'Holdings']
                revenue = shares_to_sell * current_price * (1 - commission)
                portfolio.loc[portfolio.index[i], 'Holdings'] = 0
                portfolio.loc[portfolio.index[i], 'Cash'] = portfolio.loc[portfolio.index[i-1], 'Cash'] + revenue
                position = 0
                trades.append({'Date': self.mrha_data.index[i], 'Type': 'Sell', 'Price': current_price, 'Shares': shares_to_sell})
            else:
                portfolio.loc[portfolio.index[i], 'Holdings'] = portfolio.loc[portfolio.index[i-1], 'Holdings']
                portfolio.loc[portfolio.index[i], 'Cash'] = portfolio.loc[portfolio.index[i-1], 'Cash']
            
            portfolio.loc[portfolio.index[i], 'Total_Value'] = portfolio.loc[portfolio.index[i], 'Holdings'] * current_price + portfolio.loc[portfolio.index[i], 'Cash']
            if portfolio.loc[portfolio.index[i-1], 'Total_Value'] != 0:
                portfolio.loc[portfolio.index[i], 'Returns'] = (portfolio.loc[portfolio.index[i], 'Total_Value'] / portfolio.loc[portfolio.index[i-1], 'Total_Value']) - 1
            else:
                portfolio.loc[portfolio.index[i], 'Returns'] = 0
        
        self.backtest_results = portfolio
        self.trades = pd.DataFrame(trades)

    def run_analysis(self):
        self.download_data()
        rha_data = self.calculate_revised_heikin_ashi()
        self.mrha_data = self.calculate_mrha(rha_data)
        self.add_trading_signals()
        self.calculate_price_targets()
        self.calculate_td_setup()
        self.implement_trading_logic()
        self.run_backtest()

    def get_results(self):
        total_return = (self.backtest_results['Total_Value'].iloc[-1] / self.backtest_results['Total_Value'].iloc[0]) - 1
        annualized_return = (1 + total_return) ** (252 / len(self.backtest_results)) - 1
        
        returns = self.backtest_results['Returns'].dropna()
        if len(returns) > 0 and returns.std() != 0:
            sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std()
        else:
            sharpe_ratio = 0
            
        max_drawdown = (self.backtest_results['Total_Value'] / self.backtest_results['Total_Value'].cummax() - 1).min()

        results = {
            "Final Portfolio Value": self.backtest_results['Total_Value'].iloc[-1],
            "Total Return": total_return,
            "Annualized Return": annualized_return,
            "Sharpe Ratio": sharpe_ratio,
            "Max Drawdown": max_drawdown,
            "Total Trades": len(self.trades)
        }
        return results

    def plot_results(self):
        fig = make_subplots(rows=3, cols=2, shared_xaxes=True, 
                        vertical_spacing=0.05, horizontal_spacing=0.05,
                        subplot_titles=('MRHA Chart with TD Setup', 'Backtest Results', 'Portfolio Value', '', 'Daily Returns Distribution', ''),
                        row_heights=[0.5, 0.3, 0.2], column_widths=[0.7, 0.3])

        fig.add_trace(go.Candlestick(x=self.mrha_data.index,
                    open=self.mrha_data['mh_open'],
                    high=self.mrha_data['mh_high'],
                    low=self.mrha_data['mh_low'],
                    close=self.mrha_data['mh_close'],
                    name='MRHA'), row=1, col=1)

        # TD Buy Setup 텍스트 추가
        buy_setup_text = self.mrha_data['TD_Buy_Setup'].replace(0, '').astype(str)
        buy_setup_font = ['green' if x != '9' else 'darkgreen' for x in buy_setup_text]
        buy_setup_size = [10 if x != '9' else 14 for x in buy_setup_text]

        fig.add_trace(go.Scatter(
            x=self.mrha_data.index,
            y=self.mrha_data['mh_low'] - (self.mrha_data['mh_high'] - self.mrha_data['mh_low']) * 0.05,
            text=buy_setup_text,
            mode='text',
            textposition='bottom center',
            textfont=dict(color=buy_setup_font, size=buy_setup_size),
            name='TD Buy Setup'
        ), row=1, col=1)

        # TD Sell Setup 텍스트 추가
        sell_setup_text = self.mrha_data['TD_Sell_Setup'].replace(0, '').astype(str)
        sell_setup_font = ['red' if x != '9' else 'darkred' for x in sell_setup_text]
        sell_setup_size = [10 if x != '9' else 14 for x in sell_setup_text]

        fig.add_trace(go.Scatter(
            x=self.mrha_data.index,
            y=self.mrha_data['mh_high'] + (self.mrha_data['mh_high'] - self.mrha_data['mh_low']) * 0.1,
            text=sell_setup_text,
            mode='text',
            textposition='top center',
            textfont=dict(color=sell_setup_font, size=sell_setup_size),
            name='TD Sell Setup'
        ), row=1, col=1)

        for _, trade in self.trades.iterrows():
            if trade['Type'] == 'Buy':
                fig.add_annotation(x=trade['Date'], y=self.mrha_data.loc[trade['Date'], 'mh_low'],
                               text="Buy", showarrow=True, arrowhead=1, arrowcolor="green", arrowsize=1.5,
                               arrowwidth=2, ax=0, ay=40, row=1, col=1)
            elif trade['Type'] == 'Sell':
                fig.add_annotation(x=trade['Date'], y=self.mrha_data.loc[trade['Date'], 'mh_high'],
                               text="Sell", showarrow=True, arrowhead=1, arrowcolor="red", arrowsize=1.5,
                               arrowwidth=2, ax=0, ay=-40, row=1, col=1)

        fig.add_trace(go.Scatter(x=self.backtest_results.index, y=self.backtest_results['Total_Value'],
                             mode='lines', name='Portfolio Value'), row=2, col=1)

        fig.add_trace(go.Histogram(x=self.backtest_results['Returns'].dropna(), 
                               name='Daily Returns', nbinsx=50), row=3, col=1)

        results = self.get_results()
        results_text = '<br>'.join([f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}" for key, value in results.items()])
        fig.add_annotation(text=results_text, align='left', showarrow=False, xref='paper', yref='paper', x=1.02, y=0.95, row=1, col=2)

        fig.update_layout(height=1200, width=1600, title_text=f"MRHA Trading System Results with TD Setup - {self.symbol}")
        fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
        fig.update_yaxes(title_text="Price", row=1, col=1)
        fig.update_yaxes(title_text="Portfolio Value ($)", row=2, col=1)
        fig.update_xaxes(title_text="Daily Return", row=3, col=1)
        fig.update_yaxes(title_text="Frequency", row=3, col=1)

        return fig

    def get_signals(self):
        """최근 6일간의 트레이딩 시그널을 반환합니다."""
        if self.mrha_data is None:
            return []
        
        # 날짜 기준으로 정렬된 최근 6일의 데이터 가져오기 (t-0가 가장 최근)
        recent_data = self.mrha_data.sort_index(ascending=True).tail(6)
        
        # 디버깅을 위한 날짜와 시그널 출력
        print("\nRecent dates and signals:")
        for date, signal in zip(recent_data.index, recent_data['Signal']):
            print(f"{date}: {signal}")
        
        signals = []
        for i in range(len(recent_data)):
            signal = recent_data['Signal'].iloc[i]
            if signal == 1:
                signals.append("BUY")
            elif signal == -1:
                signals.append("SELL")
            else:
                signals.append("HOLD")
        
        # 날짜 순서대로 정렬 (t-0가 가장 최근)
        return signals[::-1]  # 리스트를 역순으로 반환
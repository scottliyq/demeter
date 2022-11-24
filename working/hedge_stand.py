from datetime import date, datetime
from typing import Union
# import  matplotlib.pylab as plt
import demeter as dt
import demeter.indicator
from demeter import TokenInfo, PoolBaseInfo, Runner, Strategy, Asset, AccountStatus, BuyAction, SellAction, RowData, \
    ChainType
import numpy as np
import pandas as pd
from decimal import Decimal
import requests
import optunity
import optunity.metrics

import os
from dotenv import load_dotenv
from talib.abstract import BBANDS
import talib
from load_data import pool_id_1_eth_u_500
# import logging 
# from logging import handlers

# from strategy_ploter import  plot_position_return_decomposition


# logger = logging.getLogger()
# logger.setLevel(logging.INFO) 
# logFile = './temp/hedge.log'


# # 创建一个FileHandler,并将日志写入指定的日志文件中
# fileHandler = logging.FileHandler(logFile, mode='a')
# fileHandler.setLevel(logging.INFO) 
 
# #  或者创建一个StreamHandler,将日志输出到控制台
# streamHandler = logging.StreamHandler()
# streamHandler.setLevel(logging.INFO)

# # 定义Handler的日志输出格式
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# fileHandler.setFormatter(formatter)
 
# # 定义日志滚动条件，这里按日期-天保留日志
# timedRotatingFileHandler = handlers.TimedRotatingFileHandler(filename=logFile, when='D')
# timedRotatingFileHandler.setLevel(logging.INFO)
# timedRotatingFileHandler.setFormatter(formatter)

# # 添加Handler
# logger.addHandler(fileHandler)
# logger.addHandler(streamHandler)
# logger.addHandler(timedRotatingFileHandler)
def ema(data: pd.Series, alpha:float) -> pd.Series:
    """
    calculate simple moving average

    :param data: data
    :type data: Series
    :param alpha: ema alpha
    :type alpha: int
    :return: simple moving average data
    :rtype: Series
    """
    if data.size < 2:
        raise Exception("not enough data for simple_moving_average")
    # timespan: Timedelta = data.index[1] - data.index[0]
    # if timespan.seconds % 60 != 0:
    #     return ZelosError("no seconds is allowed")
    # span_in_minute = timespan.total_seconds() / 60
    # if unit.value % span_in_minute != 0:
    #     raise ZelosError(f"ma span is {n}{unit.name}, but data span is {span_in_minute}minute, cannot divide exactly")
    # real_n = n * int(unit.value / span_in_minute)
    # if data.size < real_n:
    #     raise ZelosError("not enough data for simple_moving_average")

    # sum = Decimal(0)
    print(f"add ema column with alpha {alpha}")
    prices_ema = data.ewm(alpha=alpha).mean()

    return prices_ema

def bollband(data: pd.Series, n=1440) -> pd.Series:
    """
    calculate simple moving average

    :param data: data
    :type data: Series
    :param n: window width, should set along with unit, eg: 5 hour, 2 minute
    :type n: int
    :param unit: unit of n, can be minute,hour,day
    :type unit: TimeUnitEnum
    :return: simple moving average data
    :rtype: Series
    """
    print(data.size)
    if data.size < 2:
        raise Exception("not enough data for simple_moving_average")
    
    upper,middle,lower = talib.BBANDS(data,timeperiod=n,nbdevup=2, nbdevdn=2, matype=talib.MA_Type.T3)
    
    serial_upper = pd.Series(upper,index=data.index).fillna(0)
    serial_middle = pd.Series(middle,index=data.index).fillna(0)
    serial_lower = pd.Series(lower,index=data.index).fillna(0)

    return serial_upper,serial_middle,serial_lower
class Exchange:


    def __init__(self, trade_symbols, leverage=20, commission=0.00005,  initial_balance=10000, log=False):
        self.initial_balance = initial_balance #初始的资产
        self.commission = Decimal(commission)
        self.leverage = leverage
        self.trade_symbols = trade_symbols
        self.date = ''
        self.log = log
        self.df = pd.DataFrame(columns=['margin','total','leverage','realised_profit','unrealised_profit'])
        self.df.index.name='timestamp'
        self.account = {'USDT':{'realised_profit':0, 'margin':0, 'unrealised_profit':0, 'total':initial_balance, 'leverage':0, 'fee':0}}
        #保存symbol的eth price和diff
        self.symbol_diff = {}
        self.symbol_single_diff = {}
        self.symbol_boll = {}
        for symbol in trade_symbols:
            self.account[symbol] = {'amount':0, 'hold_price':0, 'value':0, 'price':0, 'realised_profit':0, 'margin':0, 'unrealised_profit':0,'fee':0}
            self.symbol_diff[symbol] = pd.DataFrame(columns=['symbol','index_price','symbol_index_price','diff'])
            # self.symbol_diff[symbol].index = pd.to_datetime(price_usdt.index)

            self.symbol_single_diff[symbol] = pd.DataFrame(columns=['symbol','normal_index_price','ema_index_price','symbol_price','diff_norm','diff_ewa'])
            # self.symbol_single_diff[symbol].index = pd.to_datetime(price_usdt.index)
            self.symbol_boll[symbol] = pd.DataFrame(columns=['symbol','normal_index_price','symbol_price','std'])

    def Trade(self, symbol, direction, price, amount, msg=''):
        if self.date and self.log:
            print('%-20s%-5s%-5s%-10.8s%-8.6s %s'%(str(self.date), symbol, 'buy' if direction == 1 else 'sell', price, amount, msg))
            
        cover_amount = 0 if direction*self.account[symbol]['amount'] >=0 else min(abs(self.account[symbol]['amount']), amount)
        open_amount = amount - cover_amount
        
        self.account['USDT']['realised_profit'] -= price*amount*self.commission #扣除手续费
        self.account['USDT']['fee'] += price*amount*self.commission
        self.account[symbol]['fee'] += price*amount*self.commission
        
        if cover_amount > 0: #先平仓
            self.account['USDT']['realised_profit'] += -direction*(price - self.account[symbol]['hold_price'])*cover_amount  #利润
            self.account['USDT']['margin'] -= cover_amount*self.account[symbol]['hold_price']/self.leverage #释放保证金
            
            self.account[symbol]['realised_profit'] += -direction*(price - self.account[symbol]['hold_price'])*cover_amount
            self.account[symbol]['amount'] -= -direction*cover_amount
            self.account[symbol]['margin'] -=  cover_amount*self.account[symbol]['hold_price']/self.leverage
            self.account[symbol]['hold_price'] = 0 if self.account[symbol]['amount'] == 0 else self.account[symbol]['hold_price']
            
        if open_amount > 0:
            total_cost = self.account[symbol]['hold_price']*direction*self.account[symbol]['amount'] + price*open_amount
            total_amount = direction*self.account[symbol]['amount']+open_amount
            
            self.account['USDT']['margin'] +=  open_amount*price/self.leverage            
            self.account[symbol]['hold_price'] = total_cost/total_amount
            self.account[symbol]['amount'] += direction*open_amount
            self.account[symbol]['margin'] +=  open_amount*price/self.leverage
            
        self.account[symbol]['unrealised_profit'] = (price - self.account[symbol]['hold_price'])*self.account[symbol]['amount']
        self.account[symbol]['price'] = price
        self.account[symbol]['value'] = abs(self.account[symbol]['amount'])*price
        
        return True
    
    def Buy(self, symbol, price, amount, msg=''):
        self.Trade(symbol, 1, price, amount, msg)
        
    def Sell(self, symbol, price, amount, msg=''):
        self.Trade(symbol, -1, price, amount, msg)
        
    def Update(self, date, close_price): #对资产进行更新
        self.date = date
        # todo close price为单个close price
        self.close = close_price
        self.account['USDT']['unrealised_profit'] = 0
        for symbol in self.trade_symbols:
            # if np.isnan(close_price[symbol]):
            #     continue
            self.account[symbol]['unrealised_profit'] = (close_price - self.account[symbol]['hold_price'])*self.account[symbol]['amount']
            self.account[symbol]['price'] = close_price
            self.account[symbol]['value'] = abs(self.account[symbol]['amount'])*close_price
            self.account['USDT']['unrealised_profit'] += self.account[symbol]['unrealised_profit']
        
        self.account['USDT']['total'] = round(self.account['USDT']['realised_profit'] + self.initial_balance + self.account['USDT']['unrealised_profit'],6)
        self.account['USDT']['leverage'] = round(self.account['USDT']['margin']/self.account['USDT']['total'],4)*self.leverage
        self.df.loc[self.date] = [self.account['USDT']['margin'],self.account['USDT']['total'],self.account['USDT']['leverage'],self.account['USDT']['realised_profit'],self.account['USDT']['unrealised_profit']]

    def UpdateDiff(self, date, symbol,index_price,symbol_index_price,diff): 
        
        self.symbol_diff[symbol].loc[self.date] = [symbol,index_price,symbol_index_price,diff]

    def UpdateSingleDiff(self, date, symbol,normal_index_price,ema_index_price, symbol_price, diff_norm, diff_ewa): 
        
        self.symbol_single_diff[symbol].loc[self.date] = [symbol,normal_index_price,ema_index_price,symbol_price, diff_norm, diff_ewa]

    def UpdateBoll(self, date, symbol, normal_index_price, symbol_price, std): 
        
        self.symbol_boll[symbol].loc[self.date] = [symbol,normal_index_price,symbol_price, std]

ETH = TokenInfo(name="eth", decimal=18)
usdc = TokenInfo(name="usdc", decimal=6)


class HedgeST(dt.Strategy):
    MIN_TRADE_AMOUNT = 0.01
    hedge_count = 0
    outside_ema_count = 0
    # ema_max_spread_rate = 0
    # def set_ema_max_spread_rate(self, ema_max_spread_rate):
    #     print(f"ema_max_spread_rate: {ema_max_spread_rate}")
    #     self.ema_max_spread_rate = ema_max_spread_rate

    def __init__(self, a, hedge_spread_split,hedge_spread_rate,alpha=-1,period_n=1440,trade_symbol='ETH'):
        super().__init__()
        notice = f"init parameters: a:{a}, hedge_spread_split:{hedge_spread_split}, hedge_spread_rate:{hedge_spread_rate},alpa:{alpha},period_n:{period_n}\n"
        print(notice)
        self.a = Decimal(a)
        self.trade_symbol = trade_symbol
        self.init_quote_number = 0
        self.hedge_spread_split = hedge_spread_split
        self.hedge_spread_rate = hedge_spread_rate
        self.hedge_spread = 0
        self.hedge_amount = 0
        #init balance
        self.init_total_symbol = 0
        self.init_total_usdc = 0
        self.up_price = 0
        self.down_price = 0
        self.alpha=alpha
        self.period_n = int(period_n)

    def initialize(self):
        P0 = self.broker.pool_status.price

        status: AccountStatus = self.broker.get_account_status(P0)
        prices = self.data.closeTick.map(lambda x: self.broker.tick_to_price(x))
        if self.alpha != -1:
           
            self._add_column("ema", ema(prices,self.alpha))
        else:
            self._add_column("ema", ema(prices,0.05))
        
        # add boll band
        upper,middle,lower = bollband(prices,self.period_n)
        self._add_column("upper", upper)
        self._add_column("middle", middle)
        self._add_column("lower", lower)
        # todo fill na with 0
        self.data = self.data.fillna(0)
        future_init_net_value = status.net_value * Decimal(0.2)
        
        self.e = Exchange({self.trade_symbol},initial_balance=future_init_net_value,commission=0.00075,log=False)


        self.init_total_usdc = status.net_value + future_init_net_value
        self.init_total_symbol =  self.init_total_usdc / P0
       
        self.rebalance(P0)#rebalance all reserve token#
        # new_position(self, baseToken, quoteToken, usd_price_a, usd_price_b):
        #what is  base/quote "https://corporatefinanceinstitute.com/resources/knowledge/economics/currency-pair/"
        # print(P0)
        self.down_price = P0 / self.a
        self.up_price = P0 * self.a
        print(f"prepare to add LP: rate:{self.a} price:{P0} down:{self.down_price} up:{self.up_price} init symbol amount:{self.init_total_symbol},init usdc amount:{self.init_total_usdc}")
        self.add_liquidity(self.down_price, self.up_price)

        print("eth_value",self.broker.quote_asset.balance, "usdc value", self.broker.base_asset.balance)
        account_status = self.broker.get_account_status()
        self.init_quote_number = account_status.quote_in_position

        self.hedge_spread = self.init_quote_number / self.hedge_spread_split 
        self.hedge_amount = self.hedge_spread * Decimal(self.hedge_spread_rate)

        # price = self.broker.pool_status.price
        # self.hedge_rebalance(price, self.init_quote_number)
        # symbol = self.trade_symbol
        # print(f"{symbol}, {self.hedge_spread}")
        # e = self.e
        # e.Buy(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))


        # print(self.broker.get_account_status())
        # super().__init__()
    # def initialize(self):
    #     P0 = self.broker.pool_status.price
    #     self.rebalance(P0)#rebalance all reserve token#
    #     # new_position(self, baseToken, quoteToken, usd_price_a, usd_price_b):
    #     #what is  base/quote "https://corporatefinanceinstitute.com/resources/knowledge/economics/currency-pair/"
    #     self.add_liquidity(P0 - self.a,
    #                        P0 + self.a)
    #     print("eth_value",self.broker.quote_asset.balance)
    #     super().__init__()
    def hedge_rebalance(self, price, spot_amount_traded):
        e = self.e
        symbol = self.trade_symbol
        future_amount = e.account[symbol]['amount']
        amount_diff = future_amount - spot_amount_traded
        if amount_diff > 0:
            
            e.Sell(symbol, price, abs(amount_diff), round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
        elif amount_diff < 0:
            e.Buy(symbol, price, abs(amount_diff), round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))

        print(f"hedge rebalance {symbol} {amount_diff} {price} profit: {e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit']}")
        # print(self.broker.get_account_status())


    def next(self, row_data: Union[RowData, pd.Series]):
        # print(row_data.price)
        # if row_data.timestamp.minute != 0:
        #     return
        # print("eth_value",self.broker.quote_asset.balance, "usdc value", self.broker.base_asset.balance)
        # for position_info, position in self.broker.positions.items():
        #         print(position_info, position)  # show all position
        e = self.e

        e.Update(row_data.timestamp,row_data.price)

        price_pos = row_data.price
        if self.alpha != -1:
            # price_pos = Decimal(row_data.ema)
            # 偏离过大时候使用当前价格替代ema price
            ema_spread_rate = abs(float(row_data.price) - row_data.ema) / row_data.ema
            if ema_spread_rate >= self.ema_max_spread_rate:
                # print(f"price {row_data.price} ema {row_data.ema} too far: ema spread rate: {ema_spread_rate},ema max spread rate:{self.ema_max_spread_rate} use price")
                self.outside_ema_count +=1
                price_pos = row_data.price
            else:
                price_pos = Decimal(row_data.ema)

        current_amount = self.broker.get_account_status(price_pos).quote_in_position
        usdc_amount = self.broker.get_account_status(price_pos).base_in_position
        future_amount = self.e.account[self.trade_symbol]['amount']
        spread = self.init_quote_number*2 -current_amount - future_amount
        symbol = self.trade_symbol
        price = row_data.price

        # todo 处理价格跑出范围
        # print(f"====>rowdata.low:{row_data.low} rowdata.high:{row_data.high} rowdata.price:{row_data.price} rowdata.timestamp:{row_data.timestamp}")
        # todo row_data.low 大于 row_data.high,非ema启用
        if self.alpha==-1:
            if row_data.high < self.down_price:
                print(f"====>high:{row_data.high}, self.down_price:{self.down_price}")
                amount_down = self.broker.get_account_status(self.down_price).quote_in_position
                trade_amount = abs(self.init_quote_number*2 - amount_down - future_amount)
                if trade_amount >= self.MIN_TRADE_AMOUNT:
                    trade_price = self.down_price
                    self.hedge_count += 1
                    e.Sell(symbol, trade_price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                    print(f"{row_data.timestamp } none ema=>last hedge sell {symbol}, trade_price:{trade_price}, ema:{row_data.ema},trade_amount: {trade_amount}, current_amount: {current_amount}, hedge count:{self.hedge_count}")
            elif row_data.low > self.up_price:
                print(f"====>low:{row_data.low}, self.up_price:{self.up_price}")

                amount_up = 0
                trade_amount = self.init_quote_number*2 - future_amount
                if trade_amount >= self.MIN_TRADE_AMOUNT:
                    trade_price = self.up_price
                    self.hedge_count += 1
                    e.Buy(symbol, trade_price, abs(trade_amount), round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                    print(f"{row_data.timestamp } none ema=>last hedge buy {symbol},trade_price: {trade_price},ema:{row_data.ema}, trade_amount: {trade_amount}, current_amount: {current_amount},hedge count:{self.hedge_count}")
 
        if current_amount == 0 or usdc_amount == 0:
            # out of range, hedge at first
            #ema价格时启用
            if self.alpha!=-1:
                if spread > self.MIN_TRADE_AMOUNT:
                    self.hedge_count += 1
                    trade_amount = spread
                    e.Buy(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                    print(f"{row_data.timestamp} ema:{row_data.ema} => last hedge buy {symbol}, price: {price},ema:{row_data.ema}, trade amount:{trade_amount}, hedge count:{self.hedge_count}")
                elif spread < -1 * self.MIN_TRADE_AMOUNT:
                    self.hedge_count += 1
                    trade_amount = spread * -1
                    e.Sell(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                    print(f"{row_data.timestamp } ema:{row_data.ema} => last hedge sell {symbol},price: {price},ema:{row_data.ema}, trade amoutn: {trade_amount}, hedge count:{self.hedge_count}")


            if len(self.broker.positions) > 0:
                keys = list(self.broker.positions.keys())
                for k in keys:
                    print(f"remove lp position {k}")
                    self.remove_liquidity(k)
            print(f"{row_data.timestamp} out of range, price:{price},ema:{row_data.ema} symbol:{current_amount}, usdc:{usdc_amount}")
            self.rebalance(price)
            self.down_price = price / self.a
            self.up_price = price * self.a
            print(f"prepare to add LP: rate:{self.a} price:{price},ema:{row_data.ema}, self.down_price:{self.down_price},self.up_price { self.up_price} ")
            self.add_liquidity(self.down_price, self.up_price)
        else:
            if spread > self.hedge_spread:
                self.hedge_count += 1
                trade_amount = self.hedge_amount
                e.Buy(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                print(f"{row_data.timestamp} hedge buy {symbol}, trade price:{price},ema:{row_data.ema}, trade amount:{trade_amount}, hedge count:{self.hedge_count}")
            elif Decimal(-1)*self.hedge_spread >= spread:
                self.hedge_count += 1
                trade_amount = self.hedge_amount
                e.Sell(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                print(f"{row_data.timestamp } hedge sell {symbol},trade price: {price},ema:{row_data.ema},trade amount: {trade_amount}, hedge count:{self.hedge_count}")

        # print(f"spread:{spread}, {self.hedge_spread}")
        # print(f"spread: {spread},{self.init_quote_number},{current_amount},{future_amount}")
        # print(self.data.timestamp[row_data.row_id]) 
            # if row_data.timestamp.minute != 0:
            #     return
            # if len(self.broker.positions) > 0:
            #     keys = list(self.broker.positions.keys())
            #     for k in keys:
            #         self.remove_liquidity(k)
            #     self.rebalance(row_data.price)
            # ma_price = row_data.ma5 if row_data.ma5 > 0 else row_data.price
            # self.add_liquidity(ma_price - self.price_width,
            #                 ma_price + self.price_width)
    #重新计算并全仓入池
    def rebalance(self, price):
        status: AccountStatus = self.broker.get_account_status(price)
        # self.init_total_symbol =  status.net_value / price
        # self.init_total_usdc = status.net_value
        # print(f"net value rebalance:{status.net_value}")
        base_amount = status.net_value / 2
        quote_amount = base_amount / price
        quote_amount_diff = quote_amount - status.quote_balance
        # print(f"rebalance: {status}, ")
        if quote_amount_diff > 0:
            self.buy(quote_amount_diff)
        elif quote_amount_diff < 0:
            self.sell(0 - quote_amount_diff)
        
        self.hedge_rebalance(price, quote_amount)


def send_notice(event_name, text,text2=""):

    load_dotenv()

    ifttt_key = os.getenv('IFTTT_KEY')

    
    ifttt_key_funding_notify = ifttt_key
    key = ifttt_key_funding_notify
    url = "https://maker.ifttt.com/trigger/"+event_name+"/with/key/"+key+""
    payload = "{\n    \"value1\": \""+text+"\"\n, \"value2\": \""+text2+"\"\n}"
    headers = {
    'Content-Type': "application/json",
    'User-Agent': "PostmanRuntime/7.15.0",
    'Accept': "*/*",
    'Cache-Control': "no-cache",
    'Postman-Token': "a9477d0f-08ee-4960-b6f8-9fd85dc0d5cc,d376ec80-54e1-450a-8215-952ea91b01dd",
    'Host': "maker.ifttt.com",
    'accept-encoding': "gzip, deflate",
    'content-length': "63",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
    }
 
    requests.request("POST", url, data=payload.encode('utf-8'), headers=headers)


class HedgeSTBoll(HedgeST):
    def next(self, row_data: Union[RowData, pd.Series]):
        e = self.e
        e.Update(row_data.timestamp,row_data.price)
        price_pos = row_data.price
        price = row_data.price

        current_amount = self.broker.get_account_status(price_pos).quote_in_position
        usdc_amount = self.broker.get_account_status(price_pos).base_in_position
        future_amount = self.e.account[self.trade_symbol]['amount']
        spread = self.init_quote_number*2 -current_amount - future_amount
        symbol = self.trade_symbol
        upper = Decimal(row_data.upper)
        lower = Decimal(row_data.lower)

        if price_pos > upper and spread > self.hedge_spread:
            self.hedge_count += 1
            trade_amount = self.hedge_amount
            e.Buy(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
            print(f"{row_data.timestamp} hedge buy {symbol}, upper:{upper},lower:,{lower} trade price:{price},ema:{row_data.ema}, trade amount:{trade_amount}, hedge count:{self.hedge_count}")
        elif price_pos < lower and Decimal(-1)*self.hedge_spread >= spread:
            self.hedge_count += 1
            trade_amount = self.hedge_amount
            e.Sell(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
            print(f"{row_data.timestamp } hedge sell {symbol}, upper:{upper},lower:,{lower}, trade price: {price},ema:{row_data.ema},trade amount: {trade_amount}, hedge count:{self.hedge_count}")

        #处理跑出边界情况
        if current_amount == 0 or usdc_amount == 0:
            # out of range, hedge at first

            if spread > self.MIN_TRADE_AMOUNT:
                self.hedge_count += 1
                trade_amount = spread
                e.Buy(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                print(f"{row_data.timestamp} ema:{row_data.ema} => last hedge buy {symbol}, price: {price},ema:{row_data.ema}, trade amount:{trade_amount}, hedge count:{self.hedge_count}")
            elif spread < -1 * self.MIN_TRADE_AMOUNT:
                self.hedge_count += 1
                trade_amount = spread * -1
                e.Sell(symbol, price, trade_amount, round(e.account[symbol]['realised_profit']+e.account[symbol]['unrealised_profit'],2))
                print(f"{row_data.timestamp } ema:{row_data.ema} => last hedge sell {symbol},price: {price},ema:{row_data.ema}, trade amoutn: {trade_amount}, hedge count:{self.hedge_count}")

def backtest_boll(a, hedge_spread_split,hedge_spread_rate,boll_period_n):
    # global RUNNING_TIME
    # print(f"==================spread running time {RUNNING_TIME}==================")

    decimal_a = Decimal(a).quantize(Decimal('0.00'))
    decimal_hedge_spread_split = Decimal(hedge_spread_split).quantize(Decimal('0.0'))
    decimal_hedge_spread_rate = Decimal(hedge_spread_rate).quantize(Decimal('0.00'))

    alpha = round(0.05,4)
    ema_max_spread_rate = round(0.02,4)

    eth = TokenInfo(name="eth", decimal=18)
    usdc = TokenInfo(name="usdc", decimal=6)
    pool = PoolBaseInfo(usdc, eth, 0.05, usdc)

    #收益计算基础参数
    # net_value_base = 'ETH'

    runner_instance = Runner(pool)
    # runner_instance.enable_notify = False
    runner_instance.strategy = HedgeSTBoll(a=decimal_a,hedge_spread_split=decimal_hedge_spread_split,hedge_spread_rate=decimal_hedge_spread_rate,alpha=alpha,period_n=boll_period_n)
    runner_instance.set_assets([Asset(usdc, 10000)])
    save_path = f"../demeter/data/ETH/{pool_id_1_eth_u_500}"
    runner_instance.data_path = save_path
    runner_instance.load_data(ChainType.Ethereum.name,
                                pool_id_1_eth_u_500,
                                DATE_START,
                               DATE_END)
    runner_instance.run(enable_notify=False)

    hedge_count = runner_instance.strategy.hedge_count
    outside_ema_count = runner_instance.strategy.outside_ema_count

    # df_status = pd.DataFrame(runner_instance.account_status_list)

    total_net_value = runner_instance.final_status.net_value
    
    final_total_usdc_value = round(total_net_value + runner_instance.strategy.e.df['total'].iloc[-1],3)
    
    final_price = runner_instance.final_status.price

    final_total_eth_value = round(final_total_usdc_value / final_price,3)

    notice = f"ema+alpha:{RUNNING_TIME} times, a:{decimal_a}, hedge_spread_split:{decimal_hedge_spread_split}, hedge_spread_rate:{decimal_hedge_spread_rate},alpa:{alpha},ema rate:{ema_max_spread_rate}"
    result =f" result: outside_ema_count:{outside_ema_count},hedge count:{hedge_count} final_total_eth_value:{final_total_eth_value},final_total_usdc_value:{final_total_usdc_value}"  
    print(notice)
    print(result)
    # if SEND_NOTICE:
    #     send_notice('CEX_Notify',notice)

    # RUNNING_TIME +=1

    # if NET_VALUE_BASE == 'USDC':
    #     print(final_total_usdc_value)
    #     return float(final_total_usdc_value)
    #     # profit_rate_usdc = profit_usdc / runner_instance.strategy.init_total_usdc
    # else:
    #     print(float(final_total_usdc_value / final_price))
    #     return float(final_total_usdc_value / final_price)


    return runner_instance

if __name__ == "__main__":
    # NET_VALUE_BASE = 'ETH'
    # DATE_START = date(2022, 10, 30)
    # DATE_END = date(2022, 10, 31)
    # RUNNING_TIME = 1
    # SEND_NOTICE = True
    # # profit  = backtest(120,30,80)
    # # print(profit)
    # # profit
    # opt = optunity.maximize(backtest,  num_evals=200,solver_name='particle swarm', a=[1.12, 1.25], hedge_spread_split=[2, 3],hedge_spread_rate=[0.6, 1])



    # ########################################
    # # 优化完成，得到最优参数结果
    # optimal_pars, details, _ = opt
    # result  = f"Optimal Parameters:a={optimal_pars['a']}, hedge_spread_split={optimal_pars['hedge_spread_split']}, hedge_spread_rate={optimal_pars['hedge_spread_rate']}"
    # print(result)
    # if SEND_NOTICE:
    #     send_notice('CEX_Notify',result)

    CHAIN_ID = 'ETH'
    #POOL ID 500
    POOL_ID = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    NET_VALUE_BASE = 'ETH'
    RUNNING_TIME = 1
    SEND_NOTICE = False
    str_date_start = '2022-10-30'
    str_date_end = '2022-10-31'

    DATE_START = datetime.strptime(str_date_start, "%Y-%m-%d").date()

    DATE_END = datetime.strptime(str_date_end, "%Y-%m-%d").date()


    a = 1.20
    hedge_spread_split = 2.3
    hedge_spread_rate = 0.95
    # alpha = 0.0586
    # ema_max_spread_rate=0.027
    period_n = 240

    instance1=backtest_boll(a,hedge_spread_split,hedge_spread_rate,period_n)


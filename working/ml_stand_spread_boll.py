from hedge_stand import HedgeST,HedgeSTBoll,send_notice
from datetime import date, datetime
from demeter import TokenInfo, PoolBaseInfo, Actuator, Strategy, Asset, AccountStatus, BuyAction, SellAction, RowData, \
    ChainType
import optunity
import optunity.metrics
from decimal import Decimal
import pandas as pd
from load_data import pool_id_1_eth_u_500

import quantstats as qs
qs.extend_pandas()

def backtest_spread_boll(a, hedge_spread_split,hedge_spread_rate,period_n):
    global RUNNING_TIME
    print(f"==================spread running time {RUNNING_TIME}==={period_n}===============")

    decimal_a = Decimal(a).quantize(Decimal('0.00'))
    decimal_hedge_spread_split = Decimal(hedge_spread_split).quantize(Decimal('0.0'))
    decimal_hedge_spread_rate = Decimal(hedge_spread_rate).quantize(Decimal('0.00'))
    alpha = round(0.037, 4)
    ema_max_spread_rate = round(0.2,4)

    period_n = int(period_n)

    pool_id_tie500 = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'



    eth = TokenInfo(name="eth", decimal=18)
    usdc = TokenInfo(name="usdc", decimal=6)
    pool = PoolBaseInfo(usdc, eth, 0.05, usdc)

    #收益计算基础参数
    # net_value_base = 'ETH'
    
    runner_instance = Actuator(pool)
    # runner_instance.enable_notify = False
    runner_instance.strategy = HedgeSTBoll(a=decimal_a,hedge_spread_split=decimal_hedge_spread_split,hedge_spread_rate=decimal_hedge_spread_rate,alpha=alpha,ema_max_spread_rate=ema_max_spread_rate,period_n=period_n)
    runner_instance.set_assets([Asset(usdc, 10000)])
    save_path = f"../demeter/data/ETH/{pool_id_1_eth_u_500}"
    runner_instance.data_path = save_path
    runner_instance.load_data(ChainType.Ethereum.name,
                                pool_id_tie500,
                                DATE_START,
                               DATE_END)
    runner_instance.run(enable_notify=False)

    # df_status = pd.DataFrame(runner_instance.account_status_list)

    hedge_count = runner_instance.strategy.hedge_count

    total_net_value = runner_instance.final_status.net_value
    
    final_total_usdc_value = round(total_net_value + runner_instance.strategy.e.df['total'].iloc[-1],3)
    
    final_price = runner_instance.final_status.price

    final_total_eth_value = round(final_total_usdc_value / final_price,3)


    #处理df_status
    e = runner_instance.strategy.e
    df_status = pd.DataFrame(runner_instance.account_status_list)
    df_merge = pd.merge(e.df, df_status, on='timestamp', how='inner')


    df_merge['total_value'] = df_merge['total'] + df_merge['net_value']
    df_merge['total_value_eth'] = df_merge['total_value'] / df_merge['price']

    df_merge.set_index('timestamp', inplace=True)

    sharp_usdc = df_merge['total_value'].astype(float).sharpe()

    sharp_eth = df_merge['total_value_eth'].astype(float).sharpe()

    notice = f"spread+boll:{RUNNING_TIME} times, a:{decimal_a}, hedge_spread_split:{decimal_hedge_spread_split}, hedge_spread_rate:{decimal_hedge_spread_rate},boll period:{period_n}"
    result =f" result: hedge count:{hedge_count} final_total_eth_value:{final_total_eth_value},final_total_usdc_value:{final_total_usdc_value},sharp_usdc:{sharp_usdc},sharp_eth:{sharp_eth}"  
    print(notice)
    print(result)
    if SEND_NOTICE:
        send_notice('CEX_Notify',notice + result)

    RUNNING_TIME +=1

    if NET_VALUE_BASE == 'USDC':
        # print(final_total_usdc_value)
        # return float(final_total_usdc_value)
        # profit_rate_usdc = profit_usdc / runner_instance.strategy.init_total_usdc
        print(f"sharp_usdc:{sharp_usdc}")
        return sharp_usdc
    else:
        # print(float(final_total_usdc_value / final_price))
        # return float(final_total_usdc_value / final_price)
        # profit_rate_eth = profit_eth / runner_instance.strategy.init_total_symbol
        print(f"sharp_eth:{sharp_eth}")
        return sharp_eth
# df_status
# df

if __name__ == "__main__":
    NET_VALUE_BASE = 'ETH'
    str_date_start = '2022-8-1'
    str_date_end = '2022-10-31'
    DATE_START = datetime.strptime(str_date_start, "%Y-%m-%d").date()
    DATE_END = datetime.strptime(str_date_end, "%Y-%m-%d").date()
    RUNNING_TIME = 1
    SEND_NOTICE = True
    # profit  = backtest(120,30,80)
    # print(profit)
    # profit
    opt = optunity.maximize(backtest_spread_boll,  num_evals=100,solver_name='particle swarm', a=[1.16, 1.24], hedge_spread_split=[2.2, 2.9],hedge_spread_rate=[0.75, 1],period_n=[4, 20])



    ########################################
    # 优化完成，得到最优参数结果
    optimal_pars, details, _ = opt
    result  = f"Optimal Parameters(spread,boll) :a={optimal_pars['a']}, hedge_spread_split={optimal_pars['hedge_spread_split']}, hedge_spread_rate={optimal_pars['hedge_spread_rate']}, period_n={optimal_pars['period_n']}, from {str_date_start} to {str_date_end}"
    print(result)
    if SEND_NOTICE:
        send_notice('CEX_Notify',result)
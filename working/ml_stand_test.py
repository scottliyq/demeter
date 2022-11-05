from hedge_stand import HedgeST,send_notice
from datetime import date, datetime
from demeter import TokenInfo, PoolBaseInfo, Runner, Strategy, Asset, AccountStatus, BuyAction, SellAction, RowData, \
    ChainType
import optunity
import optunity.metrics
from decimal import Decimal


def backtest_standard(alpha):
    global RUNNING_TIME
    print(f"==================Standard running time {RUNNING_TIME}==================")

    a = 1.2
    hedge_spread_split = 3
    hedge_spread_rate = 0.8

    decimal_a = Decimal(a).quantize(Decimal('0.00'))
    decimal_hedge_spread_split = Decimal(hedge_spread_split).quantize(Decimal('0.0'))
    decimal_hedge_spread_rate = Decimal(hedge_spread_rate).quantize(Decimal('0.00'))



    RUNNING_TIME +=1
    pool_id_tie500 = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'

    pool_id_tie3000 = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'

    eth = TokenInfo(name="eth", decimal=18)
    usdc = TokenInfo(name="usdc", decimal=6)
    pool = PoolBaseInfo(usdc, eth, 0.05, usdc)

    #收益计算基础参数
    # net_value_base = 'ETH'

    runner_instance = Runner(pool)
    # runner_instance.enable_notify = False
    runner_instance.strategy = HedgeST(decimal_a,decimal_hedge_spread_split,decimal_hedge_spread_rate,alpha)
    runner_instance.set_assets([Asset(usdc, 10000)])
    runner_instance.data_path = "../demeter/data"
    runner_instance.load_data(ChainType.Ethereum.name,
                                pool_id_tie500,
                                DATE_START,
                               DATE_END)
    runner_instance.run(enable_notify=False)

    hedge_count = runner_instance.strategy.hedge_count


    notice = f"standard compare backtest {RUNNING_TIME} times, a:{decimal_a}, hedge_spread_split:{decimal_hedge_spread_split}, hedge_spread_rate:{decimal_hedge_spread_rate}, alpha:{alpha}, hedge count:{hedge_count}"
    print(notice)

    if SEND_NOTICE:
        send_notice('CEX_Notify',notice)

    return -1*hedge_count



    # df_status = pd.DataFrame(runner_instance.account_status_list)

    # total_net_value = runner_instance.final_status.net_value
    
    # final_total_usdc_value = total_net_value + runner_instance.strategy.e.df['total'].iloc[-1]
    
    # final_price = runner_instance.final_status.price
    # print(final_total_usdc_value)


    # if NET_VALUE_BASE == 'USDC':
    #     print(final_total_usdc_value)
    #     return float(final_total_usdc_value)
    #     # profit_rate_usdc = profit_usdc / runner_instance.strategy.init_total_usdc
    # else:
    #     print(float(final_total_usdc_value / final_price))
    #     return float(final_total_usdc_value / final_price)
        # profit_rate_eth = profit_eth / runner_instance.strategy.init_total_symbol
# df_status
# df

if __name__ == "__main__":
    NET_VALUE_BASE = 'ETH'
    DATE_START = date(2022, 9, 1)
    DATE_END = date(2022, 10, 31)
    RUNNING_TIME = 1
    SEND_NOTICE = True
    # hedge_count  = backtest_alpha(0.05)

    # print(profit)
    # profit
    opt = optunity.maximize(backtest_standard,  num_evals=200,solver_name='particle swarm', alpha=[0.005, 0.1])



    # ########################################
    # # 优化完成，得到最优参数结果
    optimal_pars, details, _ = opt
    result  = f"Optimal Parameters:alpha={optimal_pars['alpha']}"
    print(result)
    if SEND_NOTICE:
        send_notice('CEX_Notify',result)
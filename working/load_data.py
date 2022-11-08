from demeter.download import ChainType, DataSource, downloader
from datetime import datetime
import os
import common
import json
from pathlib import Path

pool_id_1_eth_u_500 = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'
pool_id_1_eth_u_3000 = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'


# class ChainType(Enum):
#     Ethereum = 1
#     Polygon = 2
#     Optimism = 3
#     Arbitrum = 4
#     Celo = 5
def load(start,end,chain_id,pool_id,save_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "../../env/demeter.json"

    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime( end, "%Y-%m-%d").date()
    
    chain_type = ChainType.Ethereum
    # Ethereum = 1
    # Polygon = 2
    # Optimism = 3
    # Arbitrum = 4
    # Celo = 5
    if chain_id == 'ETH':
        chain_type = ChainType.Ethereum
    elif chain_id == 'MATIC':
        chain_type = ChainType.Polygon
    elif chain_id == 'ARB':
        chain_type = ChainType.Arbitrum
    elif chain_id == 'OP':
        chain_type = ChainType.Optimism

    # ChainType.Ethereum,0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8,2022-01-01,2022-10-16,DataSource.BigQuery,./data
    downloader.download_by_day(chain_type,
                            pool_id,
                            start_date,
                            end_date,
                            DataSource.BigQuery,
                            save_path)
###################################
# main function
# python load_data.py -s 2022-11-2 -e 2022-11-7 -c ETH -p 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640
##############################
def main():

    start, end, chain_id,pool_id = common.load_param()
    if start == None or end == None or chain_id == None or pool_id == None:
        print("parameters can not be none")
        return

    print(f"{start},{end},{chain_id},{pool_id}")
    # load(start,end,chain_id,pool_id)
    from pathlib import Path
    # Path(f"../{1}/{pool_id_1_eth_u_500}").mkdir(parents=True, exist_ok=True)
    save_path = f"../demeter/data/{chain_id}/{pool_id}"
    Path(save_path).mkdir(parents=True, exist_ok=True)
    load(start,end,str(chain_id),pool_id,save_path)
if __name__ == '__main__':
    main()
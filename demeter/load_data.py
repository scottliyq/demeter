from download import ChainType, DataSource, downloader
from datetime import datetime
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "../../env/demeter.json"

pool_id_tie500 = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'

pool_id_tie3000 = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'

start_date = datetime.strptime("2022-1-1", "%Y-%m-%d").date()
end_date = datetime.strptime( "2022-11-1", "%Y-%m-%d").date()
# ChainType.Ethereum,0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8,2022-01-01,2022-10-16,DataSource.BigQuery,./data
downloader.download_by_day(ChainType.Ethereum,
                           pool_id_tie500,
                           start_date,
                           end_date,
                           DataSource.BigQuery,
                           "./data")

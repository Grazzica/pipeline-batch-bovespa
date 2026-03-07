import yfinance as yf
import pandas as pd
import boto3
from io import BytesIO

client = boto3.client('s3')
tickerStrings = ["PETR4.SA", "BOVA11.SA", "VALE3.SA", "ITSA3.SA", "IVVB11.SA"]

df= yf.download(tickerStrings, group_by='Ticker', period= '1y')

df = df.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index()

df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

df_group = df.groupby('Date')


for date, group_df in df_group:
    buffer = BytesIO()
    group_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(
        Bucket = 'tc2-bovespa-caio-2026',
        Body = buffer.read(),
        Key = f'raw/{date}/acoes.parquet'        
        )
    
    















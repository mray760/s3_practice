import os
import io
from io import StringIO
import boto3
import pandas as pd
import random
import numpy as np
import s3fs
import pandas as pd

BUCKET = "mvsgvtest"

URI = 's3://mvsgvtest/8.25_yardi_tran.xlsx'
fs = s3fs.S3FileSystem()

def write_transaction_batch(bucket: str, base_prefix: str, period: str, df: pd.DataFrame) -> str:
    def write_csv(df: pd.DataFrame, s3_uri: str) -> None:
        """Write a DataFrame to CSV in S3"""
        with fs.open(s3_uri, "w") as f:
            df.to_csv(f, index=False)


    """
    Writes a new CSV batch under raw/transactions/period=YYYY-MM/
    Returns the S3 path written.
    """
    key = f"{base_prefix}/raw/transactions/period={period}/batch=1.csv"
    s3_uri = f"s3://{bucket}/{key}"
    write_csv(df, s3_uri)
    return s3_uri


new_txns = pd.DataFrame({
        "period": ["2025-09", "2025-09", "2025-09"],
        "date": ["2025-09-05", "2025-09-06", "2025-09-07"],
        "tenant": ["Bailey, Ryan", "Bailey, Ryan", "Smith, Luana"],
        "unit_number": ["A12", "A12", "B07"],
        "account": ["Storage Revenue", "Cash", "Storage Revenue"],
        "txn_type": ["charge", "payment", "charge"],
        "amount": [70.0, 50.0, 65.0],
    })


write_transaction_batch(bucket=BUCKET,base_prefix= "Accounting",period='2025-09',df=new_txns)

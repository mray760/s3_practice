import os
import io
from io import StringIO
import boto3
import pandas as pd
import random
import numpy as np


def load_excel_from_s3(bucket: str, key: str, *, sheet_name=0, **read_excel_kwargs) -> pd.DataFrame:

    # Use default credentials (env vars, shared creds file, or IAM role)
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    with io.BytesIO(data) as bio:
        df = pd.read_csv(bio, **read_excel_kwargs)
    return df



BUCKET = "mvsgvtest"
KEY = "8.25_yardi_tran.xlsx"

df = load_excel_from_s3(BUCKET, KEY, sheet_name=0)


print(df.head())


new_period = '2025-08-01'
new_debit = np.random.randint(0, 20000)
new_credit = np.random.randint(0, 20000)

new_row = {
    "gl_code": "12345",
    "period": new_period,
    "debit": new_debit,
    "credit": new_credit,
    "description": "test row"
}

new_df = pd.DataFrame(new_row, index= [0])

new_df = pd.concat([df,new_df], ignore_index=True)

print(new_df)


# df = ...

bucket = BUCKET
key    = KEY

csv_buffer = StringIO()
new_df.to_csv(csv_buffer, index=False)
s3 = boto3.client("s3")
s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=csv_buffer.getvalue().encode("utf-8"),
    ContentType="text/csv"
)

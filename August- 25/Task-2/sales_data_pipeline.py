import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
INPUT_FILE = "sales_data.csv"
RAW_OUTPUT = "output/raw_sales_data.csv"
PROCESSED_OUTPUT = "output/processed_sales_data.csv"
os.makedirs("output", exist_ok=True)
df = pd.read_csv(INPUT_FILE)
df.to_csv(RAW_OUTPUT, index=False)

df = df.drop_duplicates(subset=["order_id"], keep="first")

df["region"] = df["region"].fillna("Unknown")
df["revenue"] = df["revenue"].fillna(0)


# Avoid division by zero
df["profit_margin"] = df.apply(
    lambda row: (row["revenue"] - row["cost"]) / row["revenue"]
    if row["revenue"] != 0 else 0,
    axis=1
)


def categorize_customer(revenue):
    if revenue > 100000:
        return "Platinum"
    elif revenue > 50000:
        return "Gold"
    else:
        return "Standard"

df["customer_segment"] = df["revenue"].apply(categorize_customer)


df.to_csv(PROCESSED_OUTPUT, index=False)
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

connection_string = (
    f"DefaultEndpointsProtocol=https;AccountName={account_name};"
    f"AccountKey={account_key};EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

def upload_to_blob(file_path, blob_name):
    try:
        with open(file_path, "rb") as data:
            container_client.upload_blob(
                name=blob_name,
                data=data,
                overwrite=True
            )
        print(f"Uploaded {blob_name} to container '{container_name}'.")
    except Exception as e:
        print(f" Failed to upload {blob_name}: {e}")

upload_to_blob(RAW_OUTPUT, "raw_sales_data.csv")
upload_to_blob(PROCESSED_OUTPUT, "processed_sales_data.csv")

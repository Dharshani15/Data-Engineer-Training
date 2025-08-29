import pandas as pd
import os
from datetime import datetime


input_file = "data/sales_data.csv"
if not os.path.exists(input_file):
    raise FileNotFoundError(f"Input file not found: {input_file}")

df = pd.read_csv(input_file)


os.makedirs("output", exist_ok=True)

df["profit_margin_percent"] = (df["profit"] / df["revenue"]) * 100
summary = df.describe(include="all")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"output/sales_report_{timestamp}.csv"
df.to_csv(output_file, index=False)

summary_file = f"output/sales_summary_{timestamp}.csv"
summary.to_csv(summary_file)


import pandas as pd
import glob
import os
from datetime import datetime

files = glob.glob("output/sales_report_*.csv")
if not files:
    raise FileNotFoundError("No sales report file found in 'output' folder. Run sales_analysis.py first.")

latest_file = max(files, key=os.path.getctime)


df = pd.read_csv(latest_file)
bottom_stores = df.nsmallest(5, "revenue")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"output/lowest_stores_{timestamp}.log"

with open(log_file, "w") as f:
    f.write("Bottom 5 Lowest Performing Stores (by revenue):\n")
    f.write(bottom_stores.to_string(index=False))



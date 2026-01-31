import pandas as pd
import os
import random

# Create input directory
os.makedirs("data/input", exist_ok=True)

# Generate Transactions CSV
data = []
for i in range(100):
    data.append({
        "transaction_id": i,
        "user_id": random.randint(1, 10), # Matches IDs from JSONPlaceholder API
        "amount": round(random.uniform(10.0, 500.0), 2),
        "date": "2023-01-01"
    })

df = pd.DataFrame(data)
df.to_csv("data/input/transactions.csv", index=False)
print("✅ Dummy data created at data/input/transactions.csv")

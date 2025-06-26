import pandas as pd
import matplotlib.pyplot as plt
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
df =  pd.read_csv(os.path.join(project_root, 'data/analysed/party_outstanding.csv'))

# Filter top 5 customers with highest dues (negative outstanding)
top_customers = df[(df['party_type'] == 'Customer') & (df['outstanding'] < 0)]
top_customers = top_customers.sort_values('outstanding').head(5)

#Plot
plt.figure(figsize=(8, 5))
plt.barh(top_customers['party_name'], -top_customers['outstanding'], color='orange')
plt.xlabel("Outstanding Amount (₹)")
plt.title("Top Outstanding Customers")
plt.tight_layout()
plt.savefig("top_customers_outstanding.png")
plt.close()

# Net Position
summary = df.groupby("party_type")["outstanding"].sum()

customer_due = abs(summary.get("Customer", 0))
supplier_due = abs(summary.get("Supplier", 0))

labels = ['Receivables (Customers)', 'Payables (Suppliers)']
sizes = [customer_due, supplier_due]  # absolute values
colors = ['skyblue', 'lightcoral']

# Plot
plt.figure(figsize=(6, 6))
plt.pie(sizes, labels=labels, colors=colors, autopct="%.1f%%", startangle=140)
plt.title("Net Business Position: Receivables vs Payables")
plt.tight_layout()
plt.savefig("net_position_pie.png")
plt.close()

# Monthly trend
mt = pd.read_csv(os.path.join(project_root, "data/analysed/monthly_trend.csv"))

plt.figure(figsize=(8, 5))
plt.plot(mt['month'], mt['total_sales'], marker='o', label='Sales', color='green')
plt.plot(mt['month'], mt['total_purchases'], marker='o', label='Purchases', color='red')
plt.fill_between(mt['month'], mt['total_sales'], mt['total_purchases'], color='lightgrey', alpha=0.3)
plt.title("Monthly Sales vs Purchases Trend")
plt.xlabel("Month")
plt.ylabel("Amount (₹)")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("monthly_trend_chart.png")
plt.close()

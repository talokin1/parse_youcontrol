# Create a sample horizontal bar chart for "TOP-5 counterparties by SUMMAEQ"
# You can replace the sample `data` with your real table or pass your own DataFrame to `plot_top5(df)`.

import pandas as pd
import matplotlib.pyplot as plt

def plot_top5(df, name_col="COUNTERPARTY", value_col="SUMMAEQ", title="ТОП-5 контрагентів за SUMMAEQ"):
    # Keep only needed columns and drop NaNs
    df = df[[name_col, value_col]].dropna()
    # Aggregate in case there are duplicates by name
    df = df.groupby(name_col, as_index=False)[value_col].sum()
    # Sort and take top 5
    df_top = df.sort_values(value_col, ascending=False).head(5)
    # Reverse for barh top-to-bottom
    df_top = df_top.iloc[::-1]

    # Plot
    plt.figure(figsize=(10, 5))
    plt.barh(df_top[name_col], df_top[value_col])
    plt.xlabel("Сума транзакцій (SUMMAEQ)")
    plt.title(title)
    # Add value labels at the end of bars
    for idx, v in enumerate(df_top[value_col]):
        plt.text(v, idx, f"{v:,.0f}", va="center", ha="left")
    plt.tight_layout()
    plt.savefig("/mnt/data/top5_summaeq.png", dpi=200)
    plt.show()

# ---- Demo with sample data ----
data = {
    "COUNTERPARTY": ["ТОВ \"Альфа\"", "ТОВ \"Бета\"", "ФОП Іваненко", "ТОВ \"Гамма\"", "ТОВ \"Дельта\"", "ТОВ \"Епсілон\""],
    "SUMMAEQ": [2300000, 1800000, 1200000, 950000, 870000, 650000]
}
df_sample = pd.DataFrame(data)
plot_top5(df_sample)

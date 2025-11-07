import pandas as pd

# === 1. Зчитуємо вихідний Excel ===
df = pd.read_excel("your_file.xlsx")

# === 2. Перетворюємо PRIMARY_SCORE у числовий формат ===
df["PRIMARY_SCORE"] = (
    df["PRIMARY_SCORE"].astype(str).str.replace("%", "").astype(float)
)

# === 3. Створюємо категорії схильності ===
def categorize(score):
    if score < 50:
        return "Low"
    elif score < 80:
        return "Medium"
    else:
        return "High"

df["PROPENSITY_LEVEL"] = df["PRIMARY_SCORE"].apply(categorize)

# === 4. Групування та агрегація ===
summary = (
    df.groupby("PROPENSITY_LEVEL")
    .agg(
        CLIENTS_COUNT=("IDENTIFYCODE", "nunique"),
        AVG_INCOME=("INCOME", "mean")
    )
    .reset_index()
    .sort_values("PROPENSITY_LEVEL", key=lambda x: x.map({"Low": 1, "Medium": 2, "High": 3}))
)

# === 5. Записуємо в новий лист ===
with pd.ExcelWriter("your_file.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    summary.to_excel(writer, sheet_name="PropensitySummary", index=False)

print("✅ Лист 'PropensitySummary' успішно додано у файл 'your_file.xlsx'")

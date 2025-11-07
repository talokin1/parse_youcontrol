import pandas as pd

# === 1. Зчитуємо початковий Excel ===
df = pd.read_excel("your_file.xlsx")

# === 2. Перетворюємо PRIMARY_SCORE у числовий формат ===
df["PRIMARY_SCORE"] = df["PRIMARY_SCORE"].astype(str).str.replace("%", "").astype(float)

# === 3. Додаємо категорію схильності ===
def categorize(score):
    if score < 50:
        return "Low"
    elif score < 80:
        return "Medium"
    else:
        return "High"

df["PROPENSITY_LEVEL"] = df["PRIMARY_SCORE"].apply(categorize)

# === 4. Створюємо зведену таблицю ===
summary = (
    df.groupby("PROPENSITY_LEVEL")
    .agg(
        CLIENTS_COUNT=("IDENTIFYCODE", "count"),
        TOTAL_EXPECTED_INCOME=("INCOME", "sum"),
        AVG_SCORE=("PRIMARY_SCORE", "mean")
    )
    .reset_index()
)

# === 5. Записуємо в новий лист Excel ===
with pd.ExcelWriter("your_file_with_summary.xlsx", engine="openpyxl", mode="w") as writer:
    df.to_excel(writer, sheet_name="RawData", index=False)
    summary.to_excel(writer, sheet_name="Summary", index=False)

print("✅ Створено файл 'your_file_with_summary.xlsx' з листом 'Summary'")

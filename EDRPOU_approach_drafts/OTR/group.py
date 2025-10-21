import pandas as pd

# Зчитуємо тільки потрібний діапазон
df = pd.read_excel(
    "your_file.xlsx",
    usecols="G:AJ",
    skiprows=235,
    nrows=56
)

# Назви регіонів беремо з першого рядка, якщо є
regions = df.columns.tolist()

# Функція для формування тексту з відсотками >7
def summarize_regions(row):
    result = []
    for region in regions:
        try:
            value = float(str(row[region]).replace('%', '').strip())
            if value > 7:
                result.append(f"{region} ({value:.0f}%)")
        except:
            continue
    return ', '.join(result) if result else '-'

# Додаємо нову колонку збоку
df['Регіони >7%'] = df.apply(summarize_regions, axis=1)

# Зберігаємо результат
df.to_excel("regions_summary.xlsx", index=False)

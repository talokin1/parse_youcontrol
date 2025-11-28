import pandas as pd
import re

kved = pd.read_excel(
    r'C:\Projects\(DS-425) Assets for inner clients\KVED_Loans.xlsx',
    dtype={'KVED': 'str'}
)

# беремо тільки рядки, де є опис нового NACE
kved = kved[~kved['NACE (from 2025)'].isna()]

# -----------------------------
# 1. Функція витягування і нормалізації КВЕД
# -----------------------------
def extract_kved(text):
    if pd.isna(text):
        return None
    text = str(text)

    # 1) Спочатку шукаємо трирівневий код: A1.1.1 → 1.11
    m3 = re.search(r'[A-Z]?(\d+)\.(\d+)\.(\d+)', text)
    if m3:
        d1, d2, d3 = m3.groups()
        # по суті це 01.11, 01.12 і т.д.
        val = float(f"{int(d1)}.{int(d2)}{int(d3)}")   # 1.11
        return str(val)                                # '1.11'

    # 2) Якщо є тільки два рівні: 1.5, 1.50, 97.0, 97.00
    m2 = re.search(r'[A-Z]?(\d+)\.(\d+)', text)
    if m2:
        d1, d2 = m2.groups()
        # 1.5 і 1.50 → float(1.5) → '1.5'
        val = float(f"{int(d1)}.{d2}")
        return str(val)

    return None


# 2. Отримати нормалізований КВЕД
kved['KVED'] = kved['NACE (from 2025)'].apply(extract_kved)

# викидаємо рядки, де КВЕД не знайшовся
kved = kved.dropna(subset=['KVED'])

# залишаємо тільки КВЕД і ризик
kved = kved[['Risk classification - Jan 2025', 'KVED']].copy()
kved.columns = ['Risk', 'KVED']

# -----------------------------
# 3. Звести дублікати: для кожного KVED взяти перший не-null Risk
# -----------------------------
risk_map = (
    kved.groupby('KVED')['Risk']
        .apply(lambda x: x.dropna().iloc[0] if x.dropna().size > 0 else None)
)

kved = (
    kved.drop_duplicates(subset=['KVED'])
        .assign(Risk=lambda df: df['KVED'].map(risk_map))
)

kved.reset_index(drop=True, inplace=True)

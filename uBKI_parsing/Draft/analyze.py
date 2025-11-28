import pandas as pd
import re

kved = pd.read_excel(
    r'C:\Projects\(DS-425) Assets for inner clients\KVED_Loans.xlsx',
    dtype={'KVED': 'str'}
)

# 1) Видалити порожні значення
kved = kved[~kved['NACE (from 2025)'].isna()]

# 2) Витягнути КВЕД із тексту
kved['KVED_CODE'] = (
    kved['NACE (from 2025)']
    .str.extract(r'([0-9][0-9]?\.\d+)')[0]
)

# ---------------------------
# 🔥 УНІВЕРСАЛЬНА НОРМАЛІЗАЦІЯ КВЕД
# ---------------------------
def normalize_kved(code):
    if pd.isna(code):
        return None
    
    # прибрати зайві пробіли
    code = str(code).strip()

    # витягти тільки числа та крапку
    match = re.findall(r'\d+', code)
    if not match:
        return None
    
    # варіанти:
    # ['1','5']  → 1.50
    # ['1','50'] → 1.50
    # ['1','5','0'] → 1.50
    if len(match) == 1:
        # тільки "1" → invalid
        return match[0]
    else:
        major = int(match[0])        # число перед точкою
        minor = int(match[1])        # число після точки
        return f"{major}.{minor:02d}"  # формат A.BB

# застосувати нормалізацію
kved['KVED_NORM'] = kved['KVED_CODE'].apply(normalize_kved)

# 3) Підготувати таблицю: KVED_NORM + Risk
df = kved[['KVED_NORM', 'Risk classification - Jan 2025']].copy()
df.columns = ['KVED', 'Risk']

# 4) Вибрати перший не-null Risk для кожного KVED
risk_map = (
    df.groupby('KVED')['Risk']
    .apply(lambda x: x.dropna().iloc[0] if x.dropna().size > 0 else None)
)

# 5) Додати фінальний Risk
df['Risk'] = df['KVED'].map(risk_map)

# 6) Видалити дублікати
df = df.drop_duplicates()

df

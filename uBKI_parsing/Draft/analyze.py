import pandas as pd
import numpy as np


file_path = r'C:\Projects\DS-425\Assets for inner clients\KVED_Loans.xlsx'
kved = pd.read_excel(file_path, dtype={'КВЕД': 'str'})

kved = kved[kved['КВЕД'] != 'Missing value']
kved = kved[~kved['КВЕД'].isna()]


kved['KVED_CODE'] = kved['NACE (from 2025)'].str.extract(r'([A-Z]?)([\d\.]{2,})')[1] 

def extract_kved_v2(text):

    if pd.isna(text):
        return None
    
    parts = text.split('.')
    num_parts = len(parts)

    if num_parts == 3:
        return parts[0] + '.' + parts[1] + parts[2]
    
    elif num_parts == 2:
      
        integer_part = parts[0]
        decimal_part = parts[1].rstrip('0') 
        
        if not decimal_part:
            return integer_part
        else:
            return integer_part + '.' + decimal_part
        
    return text

kved['KVED_v_normalized'] = kved['KVED_CODE'].astype(str).apply(extract_kved_v2)


kved_map_data = kved[['Risk classification - Jan 2025', 'KVED_v_normalized']]
kved_map_data.columns = ['Risk', 'KVED']

risk_map = (
    kved_map_data.groupby("KVED")["Risk"]
    .apply(lambda x: x.dropna().iloc[0] if not x.dropna().empty else None)
    .to_dict()
)


kved["Risk"] = kved["KVED_v_normalized"].map(risk_map)

kved.rename(columns={"KVED": "FIRM_KVED"}, inplace=True) 
kved = kved.drop_duplicates()

kved[['KVED_v_normalized', 'Risk']]
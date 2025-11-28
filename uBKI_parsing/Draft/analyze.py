import pandas as pd
import re

def find_internet_acquiring_clients(df, period):

    # ---- 1. Функція для визначення інтернет-еквайрингу ----
    def is_internet_acquiring(series: pd.Series) -> pd.Series:
        s = series.fillna("").str.lower()

        result = pd.Series(False, index=s.index)

        # укр/суржик: інтернет-екв...
        mask = s.str.contains(r"(?:інтернет|iнтернет)[\s\-_]*екв", regex=True)
        result |= mask

        # рос: интернет-экв...
        mask = s.str.contains(r"(?:интернет)[\s\-_]*экв", regex=True)
        result |= mask

        # інверсія: екв... інтернет
        mask = s.str.contains(r"(?:екв|экв).*?(?:інтернет|интернет)", regex=True)
        result |= mask

        # англ
        mask = s.str.contains(
            r"(?:internet|online|e-?commerce|ecomm)[\s\-_]*(?:acquir|gateway)",
            regex=True
        )
        result |= mask

        return result

    # ---- 2. ДЕБЕТОВІ ОПЕРАЦІЇ (клієнт списує гроші) ----
    debit = df[df["CONTRAGENTAID"].notna()].copy()
    debit["operation_type"] = "debit"  # клієнт списує

    # ---- 3. КРЕДИТОВІ ОПЕРАЦІЇ (клієнт отримує гроші) ----
    credit = df[df["CONTRAGENTBID"].notna()].copy()
    credit["operation_type"] = "credit"  # клієнт отримує

    # Об'єднуємо
    full = pd.concat([debit, credit], ignore_index=True)

    # ---- 4. Інтернет-еквайринг ----
    full["is_internet_acq"] = is_internet_acquiring(full["PLATPURPOSE"])

    # ---- 5. SELF-TRANSFER ----
    full["is_self"] = (
        full["CONTRAGENTAIDENTIFYCODE"].astype(str)
        == full["CONTRAGENTBIDENTIFYCODE"].astype(str)
    )

    # ---- 6. Відбір еквайрингу ----
    result = full[
        full["is_internet_acq"]
    ][[
        "CONTRAGENTAIDENTIFYCODE", "CONTRAGENTBIDENTIFYCODE",
        "CONTRAGENTA", "CONTRAGENTB",
        "BANKAID", "BANKBID",
        "SUMMAEQ", "PLATPURPOSE",
        "operation_type", "is_self"
    ]].copy()

    result["PERIOD"] = period
    result = result.drop_duplicates()
    return result

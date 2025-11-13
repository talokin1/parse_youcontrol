import re
import pandas as pd

def is_acquiring_only(series: pd.Series) -> pd.Series:
    s = series.fillna("").str.lower()

    result = pd.Series(False, index=s.index)

    # 1️⃣ українські + російські форми (гнучко)
    mask = s.str.contains(
        r"(екв[\W_]*[ао]?[йиiї]?[рp][иiіїy]?[ннгgґ]?[гk]?)",
        regex=True
    )
    result |= mask

    # 2️⃣ чисті російські "экв..."
    mask = s.str.contains(
        r"(экв[\W_]*[ао]?[йиi]?[рp][иi]?[нng]?[гk]?)",
        regex=True
    )
    result |= mask

    # 3️⃣ англійське acquiring
    mask = s.str.contains(
        r"\bacquir(ing|er)?\b",
        regex=True
    )
    result |= mask

    return result

df["is_acquiring_related"] = df["PLATPURPOSE"].fillna("").str.lower().str.contains(regex)

df["is_acquiring_related"] = is_acquiring_only(df["PLATPURPOSE"])

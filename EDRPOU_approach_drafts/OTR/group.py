import pandas as pd
import matplotlib.pyplot as plt

# === Дані прикладу (встав свої реальні) ===
data = {
    "CLIENT": ["ТОВ 'Альфа'", "ТОВ 'Бета'", "ТОВ 'Гамма'", "ТОВ 'Дельта'", "ФОП Іваненко"],
    "PRIMARY_SCORE": [0.9849, 0.9578, 0.9574, 0.9466, 0.9372],
    "INCOME": [28443.61, 46275.12, 21315.33, 22681.09, 12445.90],
    "NB_EMPL": [45, 28, 12, 30, 7],
    "SUMMAEQ": [442143.93, 996081.01, 115581.39, 1098747.66, 55652.89]
}
df = pd.DataFrame(data)

# === Сортуємо за PRIMARY_SCORE ===
df = df.sort_values("PRIMARY_SCORE", ascending=True)

# === Будуємо графік ===
plt.figure(figsize=(10, 5))
bars = plt.barh(df["CLIENT"], df["PRIMARY_SCORE"], color="#6DA544")

# Додаємо підписи з INCOME, NB_EMPL і SUMMAEQ
for i, (score, inc, emp, summ) in enumerate(zip(df["PRIMARY_SCORE"], df["INCOME"], df["NB_EMPL"], df["SUMMAEQ"])):
    plt.text(score + 0.001, i, f"Income: {inc:,.0f} ₴ | Emp: {emp} | SUMMAEQ: {summ:,.0f} ₴",
             va="center", fontsize=9)

plt.title("ТОП-5 контрагентів за PRIMARY_SCORE", fontsize=14, fontweight="bold")
plt.xlabel("PRIMARY_SCORE")
plt.tight_layout()
plt.savefig("/mnt/data/top5_primary_score.png", dpi=200)
plt.show()

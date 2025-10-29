import matplotlib.pyplot as plt
import pandas as pd

# Вибираємо лише потрібні колонки
cols = ["IDENTIFYCODE", "PRIMARY_SCORE", "INCOME", "NB_EMPL", "SUMMAEQ"]
df_plot = df[cols].dropna().sort_values("PRIMARY_SCORE", ascending=False).head(5)

# Робимо IDENTIFYCODE текстовими (щоб не сприймались як числа)
df_plot["IDENTIFYCODE"] = df_plot["IDENTIFYCODE"].astype(str)

# Інвертуємо порядок для горизонтального вигляду (щоб найвищий був зверху)
df_plot = df_plot.iloc[::-1]

plt.figure(figsize=(10, 5))
bars = plt.barh(df_plot["IDENTIFYCODE"], df_plot["PRIMARY_SCORE"], color="#6DA544")

# Підписи справа від барів
for i, (score, inc, emp, summ) in enumerate(zip(df_plot["PRIMARY_SCORE"], 
                                                df_plot["INCOME"], 
                                                df_plot["NB_EMPL"], 
                                                df_plot["SUMMAEQ"])):
    plt.text(score + 0.002, i, 
             f"Income: {inc:,.0f} ₴ | Emp: {emp} | SUMMAEQ: {summ:,.0f} ₴",
             va="center", fontsize=9)

plt.title("ТОП-5 контрагентів за PRIMARY_SCORE", fontsize=14, fontweight="bold")
plt.xlabel("PRIMARY_SCORE")
plt.tight_layout()
plt.show()

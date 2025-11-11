pivot = pd.pivot_table(
    merged,
    index="BANKBID",
    values="SUMMAEQ",
    aggfunc=["count", "sum"],
    margins=True,
    margins_name="TOTAL"
).round(0)

pivot.columns = ["Транзакцій", "Сума грн"]
pivot = pivot.sort_values("Сума грн", ascending=False)
print(pivot.head(15))



# ---

# за клієнтами
pivot_clients = pd.pivot_table(
    summary,
    index="CONTRAGENTAID",
    values=["total_sum", "months_active"],
    aggfunc={"total_sum": "sum", "months_active": "mean"}
).sort_values(("total_sum", ""), ascending=False)


# за місяцями
pivot_months = pd.pivot_table(
    merged,
    index="PERIOD",
    values="SUMMAEQ",
    aggfunc=["count", "sum"]
).sort_index()


with pd.ExcelWriter(r"M:\Controlling\Acquiring_Report_2025.xlsx") as writer:
    summary.to_excel(writer, sheet_name="Детальна база", index=False)
    pivot.to_excel(writer, sheet_name="Pivot за банками")
    pivot_months.to_excel(writer, sheet_name="Pivot за місяцями")

def humanize_number(value):
    """Перетворює великі числа у читабельний бізнес-формат."""
    try:
        value = float(value)
    except (ValueError, TypeError):
        return value

    if abs(value) >= 1_000_000_000:
        return f"{value/1_000_000_000:.2f}B"
    elif abs(value) >= 1_000_000:
        return f"{value/1_000_000:.2f}M"
    elif abs(value) >= 1_000:
        return f"{value/1_000:.2f}K"
    else:
        return f"{value:.2f}"


for _, row in products.iterrows():
    income = row["INCOME"]
    liabilites = row["LIABILITIES"]
    asstes = row["ASSETS"]
    credit_cards = row["CREDIT_CARDS"]
    debit_cards = row["DEBIT_CARDS"]
    cur_acc = row["CUR_ACCOUNTS"]
    savings = row["SAVINGS"]
    savings_amt = row["AMT_SAVING"]
    deposits = row["DEPOSITS"]
    deposits_amt = row["AMT_DEPOSITS"]
    digital_active = row["DIGITAL_ACTIVE"]
    client_income = row["CLIENT_INCOME"]
    swift = row["SWIFT"]
    securites = row["SECURITIES"]

    print(f"""
==============================
Income:            {humanize_number(income)} ₴
Liabilities:       {humanize_number(liabilites)} ₴
Assets:            {humanize_number(asstes)} ₴
Credit cards:      {credit_cards:.1f}
Debit cards:       {debit_cards:.1f}
Current accounts:  {cur_acc:.1f}
Savings:           {savings:.1f} ({humanize_number(savings_amt)} ₴)
Deposits:          {deposits:.1f} ({humanize_number(deposits_amt)} ₴)
Digital active:    {digital_active:.1%}
Client income:     {humanize_number(client_income)} ₴
SWIFT:             {swift:.1%}
Securities:        {humanize_number(securites)} ₴
==============================
""")

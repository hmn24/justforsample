import pandas_datareader as pdr
import pandas as pd
import time

# Reference Info: http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
df = pdr.nasdaq_trader.get_nasdaq_symbols()
df = df[~df["ETF"]]
df = df[~df["Test Issue"]]
df = df[df["Financial Status"] == "N"]

idx = df.index
basiclist = idx[~idx.str.contains("$", regex=False)]

combined_df = pd.DataFrame()
for i in basiclist:
    try:
        url = f"https://finance.yahoo.com/quote/{i}/key-statistics"
        tableParse = pd.read_html(url)
        # To prevent any rate limiting on yahoo's side
        time.sleep(5)
        df = pd.concat(tableParse)
        transposed_df = pd.DataFrame([list(df[1])], columns=df[0])
        transposed_df["Symbol"] = [i]
        if len(combined_df) == 0:
            combined_df = transposed_df
        else:
            combined_df = combined_df.append(transposed_df, ignore_index=True)
    except Exception as e:
        print(f"Exception occured: {e}")

# Generate csv to local directory
combined_df = combined_df.set_index("Symbol")
combined_df.to_csv("tabulatedData.csv", index=True)

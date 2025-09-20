import json, requests, time, glob
from datetime import datetime
import pandas as pd
import numpy as np

def price(df, interval=3600, time_frame="1H"):  # interval(秒)はtime_frame(時間足：1m,5m,15m,30m,1H,4H...)と合わせる
    with open("config.json", encoding="cp932") as f:
        api_key = json.load(f)["api_key_Birdeye"]
    if api_key == "":
        return df

    print("クリプタクト未対応コインの価格取得中...")
    token_df = pd.concat([pd.read_csv(f) for f in glob.glob("data//Token - *.csv")])
    token_df = token_df[token_df["Symbol"].str.contains("USER-", na=False)]
    addresses = token_df.set_index(token_df["Symbol"])["SPL Address"].to_dict()

    result, last_call = {}, 0
    for row in df.loc[df["コメント"] == "注意Swap", ["BlockTime", "主軸通貨"]].itertuples():
        unix_time = row.BlockTime
        symbol    = row.主軸通貨

        if abs(result.get(symbol, {}).get("time", 0) - unix_time) > interval:
            elapsed = time.perf_counter() - last_call
            time.sleep(max(0, 2 -elapsed))
            last_call = time.perf_counter()

            url = "https://public-api.birdeye.so/defi/history_price"
            params = {"address": addresses[symbol], "address_type": "token", "type": time_frame,
                "time_from": unix_time - interval, "time_to": unix_time, "ui_amount_mode": "raw"}
            headers = {"accept": "application/json", "x-chain": "solana", "X-API-KEY": api_key}

            response = requests.get(url, params=params, headers=headers, timeout=3).json()
            if not response.get("data", {}).get("items"): continue

            result[symbol] = {"price" : response["data"]["items"][0].get("value"),
                              "time"  : response["data"]["items"][0].get("unixTime")}

            date = datetime.fromtimestamp(unix_time).strftime("%Y-%m-%d")
            if result.get("jpyusd", {}).get("time") != date:
                url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/usd.json"

                response = requests.get(url, timeout=3).json()
                if not response.get("usd",{}).get("jpy"): continue

                result["jpyusd"] = {"price" : response["usd"]["jpy"], "time" : date}

        df.loc[row.Index, "価格"] = result[symbol]["price"] * result["jpyusd"]["price"]
    return df


def output(df):
    df_lending = _lending(df)
    df_solayer = _solayer(df)
    df_huma    = _huma(df)

    df_main = df[df["種類"].notna() | df.index.isin(df[df["コメント"] == "不明"].drop_duplicates("Tx").index)]
    df_main = pd.concat([df_main["Tx"], df_main.loc[:, "日時":]], axis=1)
    df_main = df_main.rename(columns={"価格": "価格（主軸通貨1枚あたりの価格）"})
    df_main["ソース"] = "Solana"

    df_main["sort"] = df_main["コメント"].map({"注意Swap": 1, "不明": 2}).fillna(0)
    df_main = df_main.sort_values(["sort", "日時"]).drop(columns="sort")

    with pd.ExcelWriter("custom.xlsx", engine="xlsxwriter") as writer:
        df_main   .to_excel(writer, sheet_name="CryptactStandard", index=False)
        df        .to_excel(writer, sheet_name="df", index=False)
        df_lending.to_excel(writer, sheet_name="Lending", index=False, header=False)
        df_solayer.to_excel(writer, sheet_name="sUSD", index=False, header=False)
        df_huma   .to_excel(writer, sheet_name="PST･mPST", index=False, header=False)

        width_settings = {"CryptactStandard": {'B': 19}, "df": {'O': 19},
                          "Lending" : {'B': 19, 'H': 19, 'N': 19, 'C': 12, 'I': 12, 'O': 12},
                          "sUSD"    : {'B': 19, 'G': 19, 'K': 19, 'N': 12},
                          "PST･mPST": {'B': 19, 'D': 19}}

        for sheet, setting in width_settings.items():
            for col, width in setting.items():
                writer.sheets[sheet].set_column(f'{col}:{col}', width)

def _lending(df):
    def process(order, original_df, service):
        columns = ["Tx", "日時", "コメント", "In Amount", "Out Amount", "Ticker"]
        df = original_df.loc[original_df["コメント"].str.contains(service), columns].copy()

        df = df[df["Ticker"] == "USDC"].sort_values("日時").reset_index(drop=True)
        df["USDC"] = df["Out Amount"].fillna(0) - df["In Amount"].fillna(0)

        formula = f"SUM({chr(62 + order * 6)}$3:{chr(62 + order * 6)}"
        df["Earned"] = [f"=IF({formula}{i+3})<0,-{formula}{i+3}),\"\")" for i in range(len(df))]
        df = df.reindex(columns=["Tx", "日時", "コメント", "USDC", "Earned", "space"])

        header = pd.DataFrame([[service] + [None]*5,
                               ["Tx", "JST", "Action", "USDC", "利息計算", None]], columns=df.columns)
        df = pd.concat([header, df], ignore_index=True)
        df = df.rename(columns={col: f"{col}_{service}" for col in df.columns})
        return df

    df_Tuna    = process(1, df, "Tuna")
    df_JupLend = process(2, df, "JupLend")
    df_Solend  = process(3, df, "Solend")

    df = pd.concat([df_Tuna, df_JupLend, df_Solend], axis=1)
    return df

def _solayer(original_df):
    columns = ["Tx", "日時", "コメント", "In Amount", "Out Amount", "Ticker"]
    df = original_df.loc[original_df["コメント"].str.contains("sUSD"), columns].copy()

    df = df[df["Ticker"].isin(["USDC", "USER-SUSD"])].sort_values(["コメント", "日時"])
    apply_df = df[df["コメント"].isin(["sUSD償還申請", "sUSD鋳造申請"])].reset_index(drop=True)
    grant_df = df[df["コメント"].isin(["sUSD償還", "sUSD鋳造"])].reset_index(drop=True)

    apply_df["Request sUSD"]  = np.where(apply_df["コメント"] == "sUSD償還申請", apply_df["Out Amount"], None)
    apply_df["Request USDC"]  = np.where(apply_df["コメント"] == "sUSD鋳造申請", apply_df["Out Amount"], None)
    grant_df["Execute sUSD"]  = np.where(grant_df["コメント"] == "sUSD償還", grant_df["In Amount"], None)
    grant_df["Execute USDC"]  = np.where(grant_df["コメント"] == "sUSD鋳造", grant_df["In Amount"], None)

    apply_df = apply_df.reindex(columns=["Tx", "日時", "Request sUSD", "Request USDC", "space"])
    grant_df = grant_df.reindex(columns=["Tx", "日時", "Execute sUSD", "Execute USDC", "space"])
    grant_df = grant_df.rename(columns={col: f"{col}_grant" for col in grant_df.columns})
    df = pd.concat([apply_df, grant_df], axis=1)

    mask = df["Request sUSD"].notna()
    df["JST"] = df["日時_grant"]
    df["buy sell"] = np.where(mask, "SELL", "BUY")
    df = df.assign(solana="Solana", base="USER-SUSD")

    df["volumn"] = np.where(mask, df["Request sUSD"], df["Execute USDC_grant"])
    df["price"] = np.where(mask, "=H" + (df.index + 3).astype(str) + "/C" + (df.index + 3).astype(str),
                                 "=D" + (df.index + 3).astype(str) + "/I" + (df.index + 3).astype(str))
    df = df.assign(counter="USDC", fee=0, jpy="JPY")

    row1 = ["申請（ガス代は計上済み）", "", "Redeem", "Mint", "", "付与", "", "Redeem", "Mint", "", "Cryptact用"] + [None]*8
    row2 = ["Tx", "JST", "sUSD", "USDC" , "", "Tx", "JST", "USDC", "sUSD", "",
            "日時", "種類", "ソース", "主軸通貨", "取引量", "価格（主軸通貨1枚あたりの価格）", "決済通貨", "手数料", "手数料通貨"]
    header = pd.DataFrame([row1, row2], columns=df.columns)
    df = pd.concat([header, df], ignore_index=True)

    return df

def _huma(original_df):
    columns = ["Tx", "日時", "コメント", "In Amount", "Out Amount", "Ticker"]
    df = original_df.loc[original_df["コメント"].str.contains("PST"), columns].copy()

    apply_df = df[df["コメント"] == "PST,mPST申請"][1::2].sort_values("日時").reset_index(drop=True)
    grant_df = df[df["コメント"] == "PST,mPST償還"][::2] .sort_values("日時").reset_index(drop=True)

    apply_df["PST"]  = np.where(apply_df["Ticker"] == "USER-PST" , apply_df["Out Amount"], None)
    apply_df["mPST"] = np.where(apply_df["Ticker"] == "USER-MPST", apply_df["Out Amount"], None)

    grant_df = grant_df.rename(columns={col: f"{col}_grant" for col in grant_df.columns})
    df = pd.concat([apply_df, grant_df], axis=1)
    df = df.reindex(columns=["Tx", "日時", "Tx_grant", "日時_grant", "PST", "mPST", "In Amount_grant"])

    header = pd.DataFrame([["申請", None, "付与", None, "申請", None, "付与"],
                           ["Tx", "JST", "Tx", "JST", "PST", "mPST", "USDC"]], columns=df.columns)
    df = pd.concat([header, df], ignore_index=True)

    return df


if __name__ == "__main__":
    import initial, analyzer

    df = initial._run_initial()

    df = analyzer.analyze(df)
    df = analyzer.copying(df)
    # df = price(df)

    output(df)

    df.to_csv("data\\after.csv", index=False, encoding='cp932')

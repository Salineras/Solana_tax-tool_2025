import json, glob
import pandas as pd
import numpy as np
from datetime import datetime, timezone ,timedelta

def extract_tx(txs):
    tx_data = []
    for tx in txs:
        if (timestamp := int(tx["timestampMs"]) // 1000) < 1735657200: continue     #2025/01/01 00:00:00 JST

        gas = tx["effects"]["gasUsed"]
        fee = int(gas["computationCost"]) + int(gas["storageCost"]) - int(gas["storageRebate"])
        balances = [balance for balance in tx["balanceChanges"] if balance["owner"].get("AddressOwner")]

        data = {"Tx"       : tx["digest"],
                "Timestamp": timestamp,
                "Error"    : tx["effects"]["status"].get("error"),
                "Fee"      : fee * 1e-9,
                "Action"   : ", ".join(event["transactionModule"] for event in tx.get("events") if event) or None,
                "PackageID": ", ".join(event["packageId"] for event in tx.get("events") if event) or None,
                "Owner"    : [balance["owner"]["AddressOwner"] for balance in balances],
                "Coin"     : [balance["coinType"] for balance in balances],
                "Amount"   : [balance["amount"]   for balance in balances]}

        tx_data.append(data)

    df = pd.DataFrame(tx_data).explode(["Owner", "Coin", "Amount"], ignore_index=True)

    addresses = json.load(open("sui\\config.json", "r"))["address"]
    maya = "0x3bf0aeb7b9698b18ec7937290a5701088fcd5d43ad11a2564b074d022a6d71ec::maya::MAYA"
    df = df[~(df["Action"].isna() & (df["Coin"] == maya) & ~df["Owner"].isin(addresses))]
    
    df["Amount"] = pd.to_numeric(df["Amount"], errors='coerce')
    return df

def JST_ticker(df):
    df = df.reindex(columns=[*df.columns, "In Amount", "Out Amount", "Ticker", "---", "日時", "種類", 
        "ソース", "主軸通貨", "取引量", "価格", "決済通貨", "手数料", "手数料通貨", "コメント"])
    string_cols = ["種類", "主軸通貨", "価格", "決済通貨", "手数料通貨"]
    df[string_cols] = df[string_cols].astype('object')

    df["日時"] = df["Timestamp"].apply(
        lambda x: (datetime.fromtimestamp(x, tz=timezone.utc) + timedelta(hours=9)).strftime("%Y/%m/%d %H:%M:%S"))

    token_df = pd.concat([pd.read_csv(f) for f in glob.glob("sui\\resources\\Token - *.csv")])
    symbol_dict  = token_df.set_index("address")["symbol"]  .to_dict()
    decimal_dict = token_df.set_index("address")["decimals"].to_dict()

    df["Ticker"]     = df["Coin"].map(symbol_dict)
    df["In Amount"]  = np.where(df["Amount"] > 0,  df["Amount"] / (10 ** df["Coin"].map(decimal_dict)), np.nan)
    df["Out Amount"] = np.where(df["Amount"] < 0, -df["Amount"] / (10 ** df["Coin"].map(decimal_dict)), np.nan)

    return df

def _run_initial():
    with open("sui\\resources\\TxData.json", "r", encoding="utf-8") as f:
        txs = json.load(f)
    df = extract_tx(txs)
    df = JST_ticker(df)
    return df

if __name__ == "__main__":
    df = _run_initial()
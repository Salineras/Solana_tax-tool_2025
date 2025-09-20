import json
import pandas as pd
import numpy as np

def analyze(df):
    addresses = json.load(open("sui\\config.json", "r"))["address"]
    words = [" bluefin", " bluemove", " cetus", " kriya_clmm", " sponsored", " router", " turbos", " universal_router"]

    rules = {
        "Swap"      : lambda g, action: action and any(word in action for word in words) and g["Owner"].iat[-1] in addresses
                                     or len(g) == 3 and g["Ticker"].iat[2] == "USER-MUSD" and action == "vault" or action == "pyth, vault",
        "Swap w/fee": lambda g, action: action and any(word in action for word in words),
        "Scallop入金": lambda g, action: len(g) == 3 and g["Ticker"].iat[2] == "USDC" and action == "mint",
        "Scallop出金": lambda g, action: len(g) == 3 and g["Ticker"].iat[2] == "USDC" and action == "redeem",
        "Kai入金"    : lambda g, action: len(g) == 3 and g["Ticker"].iat[2] == "USDC" and action == "vault",
        "Kai出金"    : lambda g, action: len(g) == 3 and g["Ticker"].iat[2] == "USDC" and action == "kai_leverage_supply_pool, vault",

        "CCTP送金"   : lambda g, action: action == "deposit_for_burn, deposit_for_burn, deposit_for_burn",
        "CCTP受取"   : lambda g, action: action == "handle_receive_message, handle_receive_message, receive_message",
        "SuiBridge送金":lambda g, action: action == "bridge",
        "SuiBridge受取":lambda g, action: action == "bridge, bridge",
        "Portal送金" : lambda g, action: action == "publish_message",
        "Portal受取" : lambda g, action: action == "complete_transfer",
        "Portal CCTP": lambda g, action: action == "deposit_for_burn, deposit_for_burn, deposit_for_burn, executor" and len(g) == 3,

        "Tx失敗"     : lambda g, action: g["Error"].iat[0] and g["Owner"].iat[0] in addresses,
        "afSUI Stake": lambda g, action: action == "staked_sui_vault, staked_sui_vault",
    }

    labels = {}
    for tx, group in df.groupby("Tx"):
        action = group["Action"].iat[0]
        labels[tx] = next((label for label, cond in rules.items() if cond(group, action)), "不明")
    df["コメント"] = df["Tx"].map(labels)

    return df

def copying(df):
    df = _copy_swap(df)

    def copy_fee(comments, step=2, action="SENDFEE", portal=False):
        idx = df.index[df["コメント"].isin(comments)][::step]
        fee = np.where(portal, -df.loc[idx, "Amount"] * 1e-9, df.loc[idx, "Fee"])

        df.loc[idx, "種類"]   = np.where(fee > 0, action, "BONUS")
        df.loc[idx, "取引量"] = np.abs(fee)
        df.loc[idx, ["主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["SUI", "JPY", 0, "JPY"]

    copy_fee(["CCTP受取", "CCTP送金", "Portal受取", "Portal送金", "SuiBridge受取", "SuiBridge送金"])
    copy_fee(["Portal CCTP"], step=3, portal=True)
    copy_fee(["Scallop入金", "Scallop出金", "Kai入金", "Kai出金"], step=3, action="DEFIFEE")

    idx = df.index[df["コメント"] == "afSUI Stake"][::2]
    df.loc[idx, "取引量"] = df.loc[idx + 1, "In Amount"].to_numpy()
    df.loc[idx, "価格"] = "=" + abs(df.loc[idx, "Out Amount"] + df.loc[idx, "Fee"]).astype(str) + "/" + df.loc[idx, "取引量"].astype(str)
    df.loc[idx, "手数料"] = df.loc[idx, "Fee"]
    df.loc[idx, ["種類", "主軸通貨", "決済通貨", "手数料通貨"]] = ["BUY", "AFSUI", "SUI", "SUI"]

    idx = df.index[df["コメント"] == "Tx失敗"]
    df.loc[idx, "取引量"] = df.loc[idx, "Fee"]
    df.loc[idx, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["DEFIFEE", "SUI", "JPY", 0, "JPY"]

    return df

def _copy_swap(df):
    updates = {}
    for _, group in df[df["コメント"].isin(["Swap", "Swap w/fee"])].groupby("Tx", sort=False):
        idx = group.index[0]
        gas = group["Fee"].iat[0]

        fee_amount, fee_ticker = None, None
        in_valid = np.where(group["In Amount"].notna())[0]
        if len(in_valid) == 0:
            in_row = 0
        elif group["コメント"].iat[0] == "Swap":
            in_row = in_valid[-1]
        else:
            in_row = in_valid[-2]
            fee_amount = group["In Amount"].iat[-1]
            fee_ticker = group["Ticker"].iat[-1]

        out_row = np.where(group["Out Amount"].notna())[0][-1]

        in_ticker  = group["Ticker"].iat[in_row]
        out_ticker = group["Ticker"].iat[out_row]
        in_amount  = group["In Amount"].iat[in_row] if in_row > 0 else abs(group["Amount"].iat[0]*1e-9 + gas)
        out_amount = group["Out Amount"].iat[out_row] if out_row > 0 else abs(group["Amount"].iat[0]*1e-9 + gas)
        if fee_ticker == out_ticker: out_amount -= fee_amount

        keys = ["種類", "主軸通貨", "取引量", "価格", "決済通貨", "手数料", "手数料通貨", "コメント"]

        if in_ticker in ["USDC", "SUI"]:
            vals = [["SELL", out_ticker, out_amount, f"={in_amount}/{out_amount}", in_ticker, gas, "SUI", None]]
        elif "USER-" not in out_ticker:
            vals = [["BUY", in_ticker, in_amount, f"={out_amount}/{in_amount}", out_ticker, gas, "SUI", None]]
        elif "USER-" not in in_ticker:
            vals = [["SELL", out_ticker, out_amount, f"={in_amount}/{out_amount}", in_ticker, gas, "SUI", None]]
        else:
            vals = [["BUY", in_ticker, in_amount, None, "JPY", gas, "SUI", "注意Swap"],
                    ["SELL", out_ticker, out_amount, None, "JPY", 0, "SUI", "注意Swap"]]

        if gas < 0:
            vals.append(["BONUS", "SUI", -gas, None, "JPY", 0, "JPY", None])
            vals[0][5:7] = [0, "JPY"]

        if group["コメント"].iat[0] == "Swap w/fee":
            vals.append(["DEFIFEE", fee_ticker, fee_amount, None, "JPY", 0, "JPY", None])

        for i, vals in enumerate(vals):
            updates[idx + i] = dict(zip(keys, vals))

    df.update(pd.DataFrame.from_dict(updates, orient="index"))
    return df


if __name__ == "__main__":
    import initial

    df = initial._run_initial()
    df = analyze(df)
    copying(df)
    # df.to_csv("sui\\after.csv", index=False, encoding='cp932')
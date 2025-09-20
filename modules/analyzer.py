import requests, json, glob, sys, time
import pandas as pd
import numpy as np

def analyze(df):
    temp_df  = pd.concat([pd.read_csv(p) for p in glob.glob("data//Token - *.csv")])
    my_ATAs  = set(temp_df["Account"].to_list())
    cex_ATAs = _cex_address()

    rules = {
        "ダスト攻撃"  : lambda g, action: action == "Dusting Attack",
        "Tx失敗"     : lambda g, action: g["Error"].iat[0],
        "Swap"       : lambda g, action: action in ("Fill", "Route", "Route, PlaceTakeOrder") or action and "Swap" in action and "InitializeAccount3" not in action,
        "Swap,rent代": lambda g, action: action and ("InitializeImmutableOwner, InitializeAccount3, SwapRoute, Swap" in action or "InitializeImmutableOwner, InitializeAccount3, Route, Swap" in action) and "CreateTokenAccount" not in action,
        "Jito tip"   : lambda g, action: action is None and len(g) == 1 and (g["Destination"].iat[0]) in JITO_TIP,

        "ワムホ準備1" : lambda g, action: action is None and (len(g) == 1 and g["Type"].iat[0] == "WormholeCoreBridge"),
        "ワムホ準備2" : lambda g, action: action is None and (len(g) == 2 and g["Type"].iat[1] == "WormholeCoreBridge"),
        "ワムホ受取"  : lambda g, action: action is None and (g["Type"] == "mintTo").sum() == 2 and g["Amount"].iat[0] == 897840,
        "ワムホ送金"  : lambda g, action: action == "Approve" and len(g) == 4 and g["Type"].iat[2] == "WormholeCoreBridge",
        "CCTP送金"   : lambda g, action: action == "DepositForBurn, SendMessage",
        "CCTP受取"   : lambda g, action: action == "ReceiveMessage, HandleReceiveMessage",
        "CCTP自動受取": lambda g, action: action == "CCTP-AutoClaim",
        "CCTP Portal": lambda g, action: action == "DepositForBurn, SendMessage, RelayLastMessage, RequestForExecution",
        "PST,mPST申請": lambda g, action: action == "AddRedemptionRequest",
        "PST,mPST償還": lambda g, action: action == "ProcessRedemptionRequest",
        "sUSD償還申請": lambda g, action: action == "Withdraw" and len(g) == 2 and g["Destination"].iat[1] == "FfELPrBpJBzyND1xTM6NmXRaQJbuP8c5J8jmJWZvDJds",
        "sUSD償還"   : lambda g, action: action and "ResolveBatchWithdraw" in action,
        "Tuna入金"   : lambda g, action: action == "Deposit"  and g["Destination"].iat[0] == "4iTbtBmr4fXpkUD4kTW9pujvXbCT3AkWya6h3dbNP7a6",
        "Tuna出金"   : lambda g, action: action == "Withdraw" and g["Source"].iat[0] == "4iTbtBmr4fXpkUD4kTW9pujvXbCT3AkWya6h3dbNP7a6",
        "Tuna初回入金": lambda g, action: action == "OpenLendingPosition, Deposit" and g["Destination"].iat[1] == "4iTbtBmr4fXpkUD4kTW9pujvXbCT3AkWya6h3dbNP7a6",
        "JupLend入金": lambda g, action: action == "Deposit, PreOperate, Operate",
        "JupLend出金": lambda g, action: action == "Withdraw, Operate",
        "JupLend初回": lambda g, action: action == "Deposit, GetAccountDataSize, InitializeImmutableOwner, InitializeAccount3, PreOperate, Operate",

        "Solend入金3": lambda g, action: action and len(g) == 3 and "Deposit Reserve Liquidity and Obligation Collateral" in action,
        "Solend入金4": lambda g, action: action and len(g) == 4 and "Deposit Reserve Liquidity and Obligation Collateral" in action,
        "Solend出金3": lambda g, action: action and len(g) == 3 and "Withdraw Obligation Collateral and Redeem Reserve Collateral" in action,
        "Solend出金4": lambda g, action: action and len(g) == 4 and "Withdraw Obligation Collateral and Redeem Reserve Collateral" in action,
        "CEX出金"    : lambda g, action: action is None and g["Source"].iat[0] in cex_ATAs,
        "CEX入金"    : lambda g, action: action is None and g["Destination"].iat[0] in cex_ATAs,
        "CEX初入金"  : lambda g, action: action == "GetAccountDataSize, InitializeImmutableOwner, InitializeAccount3" and len(g) == 2 and g["Destination"].iat[1] in cex_ATAs,
        "自アドレス間": lambda g, action: action is None and g["Source"].iat[0] in my_ATAs and g["Destination"].iat[0] in my_ATAs,
        "sUSD鋳造申請": lambda g, action: action == "Deposit" and len(g) == 2 and g["Destination"].iat[1] == "DN2KzAeiHFnndVpjWSTw8UkwF9PZaFSthSnVoW1MVThQ",
        "sUSD鋳造"    : lambda g, action: action == "ResolveBatchDeposit, TransferHook",
        "Jupiter投票" : lambda g, action: action == "NewVote, CastVote, SetVote",
        "JUPロック等" : lambda g, action: action in ("OpenPartialUnstaking", "MergePartialUnstaking", "ToggleMaxLock","WithdrawPartialUnstaking", "IncreaseLockedAmount"),
        "エアドロ"    : lambda g, action: action == "GetAccountDataSize, InitializeImmutableOwner, InitializeAccount3, NewClaim",
        "Jup指値"     : lambda g, action: action == "InitializeOrder, GetAccountDataSize, InitializeImmutableOwner, InitializeAccount3",
        "Jup指値取消" : lambda g, action: action == "CancelOrder",
        "PST,mPST鋳造": lambda g, action: action == "Deposit" and g["Destination"].iat[0] == "6Xh2Jg9sWJE16VQGppJFTHvQ8Vii3ABUvUF8Pwcwy7Vq",
        "Switch Mode": lambda g, action: action == "SwitchMode",
        "Solend手数料1": lambda g, action: action == "WriteEncodedVaa, VerifyEncodedVaaV1, UpdatePriceFeed, CloseEncodedVaa",
        "Solend手数料2": lambda g, action: action == "InitEncodedVaa, WriteEncodedVaa"}

    labels = {}
    for tx, group in df.groupby("Tx"):
        action = group["Action"].iat[0]
        labels[tx] = next((label for label, cond in rules.items() if cond(group, action)), "不明")
    df["コメント"] = df["Tx"].map(labels)

    return df

def _cex_address():
    with open("config.json", "r", encoding="cp932") as f:
        config  = json.load(f)
        api_key = config["api_key_Helius"]
        CEX_add = config["CEX withdraw address"] + config["CEX deposit address"]

    cex_ATAs = []
    for address in CEX_add:
        url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        payload = {"jsonrpc": "2.0", "id": "1", "method": "getTokenAccountsByOwner",
            "params": [address, {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}, {"encoding": "jsonParsed"}]}

        for attempt in range(3):
            try:
                response = requests.post(url, json=payload, timeout=8).json()
            except (requests.Timeout, requests.RequestException):
                time.sleep(1)
            else:
                if not response.get("result", {}).get("value", []):
                    print(f"CEX ATA 取得失敗：{address}")
                else:
                    if attempt > 0:
                        print(f"【注意】Heliusリトライ実施、CEX入手金処理に不備の可能性あり CEX address={address}")
                    cex_ATAs.extend(res["pubkey"] for res in response["result"]["value"])
                break
        else:
            print(f"Heliusリトライ失敗で強制終了 CEX address = {address}")
            sys.exit()

    return set(cex_ATAs)


def copying(df):
    df = _copy_swap(df)
    df = _copy_fee(df)

    idx = df.index[df["コメント"] == "ダスト攻撃"]
    df.loc[idx, ["取引量", "主軸通貨"]] = df.loc[idx, ["In Amount", "Ticker"]].to_numpy()
    df.loc[idx, ["種類", "決済通貨", "手数料", "手数料通貨"]] = ["BONUS", "JPY", 0, "JPY"]

    idx = df.index[df["コメント"] == "エアドロ"][::3]
    df.loc[idx, ["取引量", "主軸通貨"]] = df.loc[idx + 2, ["In Amount", "Ticker"]].to_numpy()
    df.loc[idx, ["種類", "決済通貨", "手数料", "手数料通貨"]] = ["BONUS", "JPY", 0, "JPY"]
    df.loc[idx + 1, "取引量"] = df.loc[idx + 1, "Fee"] + df.loc[idx + 1, "Out Amount"] + df.loc[idx, "Out Amount"].to_numpy()
    df.loc[idx + 1, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["DEFIFEE", "SOL", "JPY", 0, "JPY"]

    for _, group in df[df["コメント"] == "Poisoning"].groupby("Tx", sort=False):
        idx  = group.index.values[0]
        df.at[idx, "取引量"]   = group["In Amount"].fillna(0).sum()
        df.at[idx, "主軸通貨"] = group["Ticker"].dropna().iat[0]
        df.loc[idx, ["種類", "決済通貨", "手数料", "手数料通貨"]] = ["BONUS", "JPY", 0, "JPY"]

    idx = df[df["コメント"] == "PST,mPST償還"].groupby("Tx", sort=False).head(1).index
    mPST = "HUPfpnsaJtJGpJxAPNX1vXah7BgYiQYt1c2JMgMumvPs"
    
    df.loc[idx, "主軸通貨"] = np.where(df.loc[idx + 1, "Mint"] == mPST, "USER-MPST", "USER-PST")
    df.loc[idx, "取引量"] = (amount := df.loc[idx + 1, "Amount"].to_numpy() * 1e-6)
    df.loc[idx, "価格"] = "=" + df.loc[idx, "In Amount"].astype(str) + "/" + amount.astype(str)
    df.loc[idx, ["種類", "決済通貨", "手数料", "手数料通貨"]] = ["SELL", "USDC", 0, "JPY"]

    df.loc[idx + 1, "Ticker"] = df.loc[idx, "主軸通貨"].to_numpy()
    df.loc[idx + 1, "Out Amount"] = amount

    return df

def _copy_swap(df):
    updates = {}
    swap_groups = df[df["コメント"].isin(["Swap" ,"Swap,rent代", "Switch Mode", "PST,mPST鋳造"])].groupby("Tx", sort=False)

    for _, group in swap_groups:
        first_idx  = group.index.values[0]
        second_idx = first_idx + 1

        in_amounts  = group["In Amount"].values
        out_amounts = group["Out Amount"].values
        in_valid    = ~np.isnan(in_amounts)
        out_valid   = ~np.isnan(out_amounts)

        in_ticker   = group["Ticker"].values[in_valid][0]
        out_tickers = group["Ticker"].values[out_valid]
        out_ticker  = out_tickers[1] if group["コメント"].iat[0] == "Swap,rent代" else out_tickers[0]

        jito_tip = out_amounts[-1] if (group["Destination"].iat[-1]) in JITO_TIP else 0
        rent_fee = out_amounts[0] if group["コメント"].iat[0] == "Swap,rent代" else 0

        jupiter_fee = 0
        if out_valid.sum() >= 2 and in_ticker in out_tickers:
            jupiter_fee = out_amounts[out_valid][-2] if jito_tip else out_amounts[out_valid][-1]

        net_in  = in_amounts[in_valid].sum()
        net_out = out_amounts[out_valid].sum() - jito_tip - jupiter_fee - rent_fee
        net_fee = group["Fee"].iat[0] + jito_tip + rent_fee

        keys    = ["種類", "主軸通貨", "取引量", "価格", "決済通貨", "手数料", "手数料通貨"]
        ex_keys = ["種類", "主軸通貨", "取引量",         "決済通貨", "手数料", "手数料通貨", "コメント"]

        if "USDC" == in_ticker:
            vals = ["SELL", out_ticker, net_out, f"={net_in}/{net_out}", in_ticker, net_fee, "SOL"]
            updates[first_idx] = dict(zip(keys, vals))
        
        elif "USER-" not in out_ticker:
            vals = ["BUY", in_ticker, net_in, f"={net_out}/{net_in}", out_ticker, net_fee, "SOL"]
            updates[first_idx] = dict(zip(keys, vals))

        elif "USER-" not in in_ticker:
            vals = ["SELL", out_ticker, net_out, f"={net_in}/{net_out}", in_ticker, net_fee, "SOL"]
            updates[first_idx] = dict(zip(keys, vals))

        else:
            vals_buy  = ["BUY", in_ticker, net_in, "JPY", net_fee, "SOL", "注意Swap"]
            vals_sell = ["SELL", out_ticker, net_out, "JPY", 0, "SOL", "注意Swap"]
            updates[first_idx]  = dict(zip(ex_keys, vals_buy))
            updates[second_idx] = dict(zip(ex_keys, vals_sell))

        if jupiter_fee > 0:
            fee_idx = second_idx + 1 if ("USER-" in in_ticker) and ("USER-" in out_ticker) else second_idx
            vals = ["DEFIFEE", in_ticker, jupiter_fee, "JPY", 0, "JPY", "Jupiter手数料"]
            updates[fee_idx] = dict(zip(ex_keys, vals))

    df_updates = pd.DataFrame.from_dict(updates, orient='index')
    df.update(df_updates.reindex(df.index))
    return df

def _copy_fee(df):
    def process(comments, step=1, amount=True, fee="DEFIFEE"):
        mask = df["コメント"].isin(comments)
        idx = df.index[mask][::step]
        df.loc[idx, "取引量"] = df.loc[idx, "Fee"] + (df.loc[idx, "Out Amount"] if amount else 0)
        df.loc[idx, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = [fee, "SOL", "JPY", 0, "JPY"]

    process(["Jito tip", "Jupiter投票", "Solend手数料2"])
    process(["JUPロック等", "Tuna出金", "Tuna入金", "Solend手数料1"], amount=False)
    process(["sUSD鋳造申請", "sUSD償還申請", "Tuna初回入金"], step=2)
    process(["PST,mPST申請", "Jup指値取消", "JupLend入金", "JupLend出金"], step=2, amount=False)
    process(["JupLend初回"], step=3)
    process(["Jup指値", "Solend出金3", "Solend入金3"], step=3, amount=False)

    process(["CEX入金", "CCTP受取", "ワムホ準備1", "自アドレス間"], amount=False, fee="SENDFEE")
    process(["ワムホ準備2", "CEX初入金"], step=2, fee="SENDFEE")
    process(["ワムホ受取"], step=3, fee="SENDFEE")

    idx = df.index[df["コメント"].isin(["Solend出金4", "Solend入金4"])][::4]
    df.loc[idx, "取引量"] = df.loc[idx, "Fee"] + df.loc[idx + 3, "Out Amount"].to_numpy()
    df.loc[idx, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["DEFIFEE", "SOL", "JPY", 0, "JPY"]

    idx = df.index[df["コメント"] == "CCTP送金"][::2]
    df.loc[idx, "取引量"] = df.loc[idx, "Fee"] + df.loc[idx + 1, "Out Amount"].to_numpy()
    df.loc[idx, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["SENDFEE", "SOL", "JPY", 0, "JPY"]

    idx = df.index[df["コメント"] == "ワムホ送金"][::4]
    df.loc[idx, "取引量"] = df.loc[idx, "Fee"] + df.loc[idx + 1, "Out Amount"].to_numpy() + df.loc[idx + 3, "Out Amount"].to_numpy()
    df.loc[idx, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["SENDFEE", "SOL", "JPY", 0, "JPY"]

    idx = df[df["Error"].notna()].groupby("Tx", sort=False).head(1).index
    df.loc[idx, "取引量"] = df.loc[idx, "Fee"]
    df.loc[idx, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["DEFIFEE", "SOL", "JPY", 0, "JPY"]

    for _, group in df[df["コメント"] == "CCTP Portal"].groupby("Tx", sort=False):
        first_idx  = group.index.values[0]
        second_idx = first_idx + 1

        df.at[first_idx, "取引量"] = df.at[first_idx, "Fee"] + df.at[second_idx, "Out Amount"] + df.at[second_idx + 1, "Out Amount"]
        df.loc[first_idx, ["種類", "主軸通貨", "決済通貨", "手数料", "手数料通貨"]] = ["SENDFEE", "SOL", "JPY", 0, "JPY"]

        if len(group) == 4:
            df.at[second_idx, "取引量"]  = df.at[first_idx + 3, "Out Amount"]
            df.at[second_idx, "主軸通貨"] = df.at[first_idx + 3, "Ticker"]
            df.loc[second_idx, ["種類", "決済通貨", "手数料", "手数料通貨"]] = ["SENDFEE", "JPY", 0, "JPY"]

    return df


JITO_TIP = {"96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"}

if __name__ == "__main__":
    import initial

    df = initial._run_initial()
    df = analyze(df)
    copying(df)
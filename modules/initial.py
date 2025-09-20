import requests, json, time, sys, glob
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timezone ,timedelta

def fetch_tx(limit=1000, cutoff_time=1735657200):     #2025/01/01 00:00:00 JST
    with open("config.json", "r", encoding="cp932") as f:
        api_key = json.load(f)["api_key_Alchemy"]
    url = f"https://solana-mainnet.g.alchemy.com/v2/{api_key}"

    df = pd.concat([pd.read_csv(p) for p in glob.glob("data//Token - *.csv")])
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    sUSD = "susdabGDNbhrnCa6ncrYo81u4s9GM8ecK2UwMyZiq4X"
    addresses = df.loc[(df["SPL Address"].isin(["-", usdc, sUSD])) , "Account"].to_list()

    print("Tx一覧取得中...")
    tx_lists = []
    for address in addresses:
        payload  = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [address, {"limit": limit}]}

        checker = float("inf")
        while checker > cutoff_time:
            response = requests.post(url, json=payload).json()

            result  = response.get("result", [])
            if not result:
                print(f"Tx一覧取得失敗？  総取得数：{len(tx_lists)}  アドレス：{address}")
                break

            tx_lists.extend(result)
            payload["params"][1]["before"] = result[-1]["signature"]
            checker = result[-1]["blockTime"]
            time.sleep(0.3)

    tx_lists = [tx["signature"] for tx in tx_lists if tx["blockTime"] >= cutoff_time]
    tx_lists = list(dict.fromkeys(tx_lists))

    return tx_lists, url

def fetch_tx_data(tx_lists, url, interval=10, batch_size=100):
    all_txs =[]

    for i in tqdm(range(0, len(tx_lists), batch_size), leave=False, desc= "Tx取得状況"):
        payload = [{"jsonrpc": "2.0", "id": idx + 1, "method": "getTransaction",
                    "params": [tx, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]}
                    for idx, tx in enumerate(tx_lists[i:i + batch_size])]
        
        start    = time.perf_counter()
        response = requests.post(url, json=payload).json()
        elapsed  = time.perf_counter() - start

        for res in response:
            if "error" in res:
                print(f"！！！ APIエラー：{res['error']['message']}")
                sys.exit()

            if not res.get("result"):
                print(f"！！！ Tx取得失敗 ！！！")
                continue

            all_txs.append(res["result"])

        time.sleep(max(0, interval - elapsed))

    return all_txs


def extract_tx(txs):
    tx_data = []
    for tx in txs:
        if not tx.get("meta"): continue
        instruction_data = []

        for innerInstruction in tx["meta"].get("innerInstructions", []):
            for instruction in innerInstruction.get("instructions", []):
                if result := _get_instruction(instruction):
                    instruction_data.append(result)

        message = tx["transaction"].get("message", {})
        for instruction in message.get("instructions", []):
            if result := _get_instruction(instruction):
                instruction_data.append(result)

        exclusion = "Transfer", "TransferChecked", "CreateAccount", "CloseAccount", "MintTo", "Burn", "SharedAccountsRoute"
        logMessages = ", ".join(log[26:] for log in tx["meta"]["logMessages"]
                      if "Program log: Instruction:" in log and log[26:] not in exclusion) # log[26:] "Program log: Instruction: "以降
        
        if logMessages == "ReceiveMessage, HandleReceiveMessage" and message["accountKeys"][0]["pubkey"] == "eHgWQRohuMfdxPuXUhgMswMs3zbXMKtRRzKkNde76X5":
            logMessages = "CCTP-AutoClaim"

        temp = {"Tx"       : tx["transaction"]["signatures"][0],
                "BlockTime": tx["blockTime"],
                "Error"    : tx["meta"]["err"],
                "Fee"      : tx["meta"]["fee"] * 1e-9,
                "Action"   : logMessages if logMessages != "" else None}

        for key in ["Type", "Source", "Destination", "Mint", "Amount"]:
            temp[key] = [data[key.lower()] for data in instruction_data]
        tx_data.append(temp)

    df = pd.DataFrame(tx_data)
    return df

def _get_instruction(instruction):
    if instruction.get("programId") == "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY":
        if len(instruction.get("accounts", [])) >= 5:
            return {
                "type"       : "MintToCollectionV1",
                "source"     : instruction["accounts"][4],
                "destination": instruction["accounts"][2],
                "amount"     : None,
                "mint"       : None
            }
        return None
    elif instruction.get("programId") == "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth":
        return {"type": "WormholeCoreBridge", "source": None, "destination": None, "amount": None, "mint": None}

    if not isinstance(instruction.get("parsed"), dict): # parsedにメモがあるときの対策
        return None

    info    = instruction.get("parsed", {}).get("info")
    tx_type = instruction.get("parsed", {}).get("type")
    
    if tx_type in ["transfer", "transferChecked", "createAccount", "closeAccount", "mintTo", "burn"]:
        return {
            "type"       : tx_type,
            "source"     : info.get("source")      or (info["account"] if tx_type == "burn"   else None),
            "destination": info.get("destination") or (info["account"] if tx_type == "mintTo" else None),
            "amount"     : info.get("amount") or info.get("lamports") or info.get("tokenAmount", {}).get("amount"),
            "mint"       : info.get("mint") or info.get("tokenAmount", {}).get("mint")
        }
    
    return None


def filter_scam(df):
    with open("config.json", encoding="cp932") as f:
        addresses = set(json.load(f)["address"])

    with open("data\\scam.txt", encoding="utf-8") as f:
        scammers  = set(line.strip() for line in f if line.strip() and not line.startswith("#"))

    scam_mask = df["Source"].apply(lambda s: bool(s) and (s[0] in scammers))

    for idx in df[scam_mask].index:
        if df.at[idx, "Source"] and df.at[idx, "Source"][0] in scammers:
            indices = [i for i, dest in enumerate(df.at[idx, "Destination"]) if dest in addresses]
            if indices:
                for col in ["Source", "Destination", "Type", "Amount", "Mint"]:
                    df.at[idx, col] = [df.at[idx, col][i] for i in indices]

    df.loc[scam_mask, "Action"] = "Dusting Attack"
    dusting_attack = scam_mask & df["Error"].notna()
    cNFT_attack    = scam_mask & df["Type"].apply(lambda type: any(t == "MintToCollectionV1" for t in type))

    df = df[~(dusting_attack | cNFT_attack)].reset_index(drop=True)
    return df

def JST_ticker(df):
    df = df.explode(["Type", "Source", "Destination", "Amount", "Mint"], ignore_index=True)
    df["Amount"] = pd.to_numeric(df["Amount"], errors='coerce')

    df = df.reindex(columns=[*df.columns, "In Amount", "Out Amount", "Ticker", "---", "日時", "種類", 
        "ソース", "主軸通貨", "取引量", "価格", "決済通貨", "手数料", "手数料通貨", "コメント"])
    string_cols = ["種類", "主軸通貨", "価格", "決済通貨", "手数料通貨"]
    df[string_cols] = df[string_cols].astype('object')

    df["日時"] = df["BlockTime"].apply(
        lambda x: (datetime.fromtimestamp(x, tz=timezone.utc) + timedelta(hours=9)).strftime("%Y/%m/%d %H:%M:%S"))

    token_df = pd.concat([pd.read_csv(f) for f in glob.glob("data\\Token - *.csv")])
    account_dict = token_df.set_index("Account")["Symbol"] .to_dict()
    decimal_dict = token_df.set_index("Symbol")["decimals"].to_dict()

    df["Ticker_from_src"]  = df["Source"]     .map(account_dict)
    df["Ticker_from_dest"] = df["Destination"].map(account_dict)

    df["Ticker"] = df["Ticker_from_src"].fillna(df["Ticker_from_dest"])
    df["In Amount"]  = (df["Amount"] / (10 ** df["Ticker"].map(decimal_dict))).where(df["Ticker_from_dest"].notna())
    df["Out Amount"] = (df["Amount"] / (10 ** df["Ticker"].map(decimal_dict))).where(df["Ticker_from_src"].notna())

    df.drop(columns=["Ticker_from_src", "Ticker_from_dest"], inplace=True)
    return df


def _run_initial():
    with open("data\\TxData.json", "r", encoding="utf-8") as f:
        all_txs = json.load(f)
    df = extract_tx(all_txs)
    df = filter_scam(df)
    df = JST_ticker(df)
    return df

if __name__ == "__main__":
    tx_lists, url = fetch_tx()
    all_txs       = fetch_tx_data(tx_lists, url)

    open("data\\TxList.txt", "w").write("\n".join(tx_lists))
    with open("data\\TxData.json", "w", encoding="utf-8") as f:
        json.dump(all_txs, f, indent=2)

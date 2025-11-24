import json, glob, requests, time, sys, asyncio, aiohttp
import pandas as pd
import numpy as np
from tqdm.asyncio import tqdm
from datetime import datetime, timezone ,timedelta

def fetch_digest(cutoff_time=1735657200):     #2025/01/01 00:00:00 JST
    with open("sui/config.json", "r", encoding="utf-8") as f:
        config    = json.load(f)
        api_key   = config["api_key_Shinami"]
        addresses = config["address"]

    url = f"https://api.apac1.shinami.com/sui/node/v1/{api_key}"

    print("Tx取得中...")
    digests = []
    for address in addresses:
        for filter in ["FromAddress", "ToAddress"]:
            params = [{"filter": {filter: address}}, None, 50, True] # cursor, limit, descending_order
            payload = {"jsonrpc": "2.0", "id": 1, "method": "suix_queryTransactionBlocks", "params": params}

            checker = float("inf")
            while checker > cutoff_time:
                result = requests.post(url, json=payload, timeout=5)
                if result.status_code != 200:
                    print(f"Tx一覧取得失敗？ error: {result.status_code}  address: {address}"); break

                digests.extend(result.json()["result"]["data"])
                if result.json()["result"]["hasNextPage"] == False: break

                params[1] = digests[-1]["digest"]
                header = {"jsonrpc": "2.0", "id": 1, "method": "sui_getTransactionBlock", "params": [params[1]]}

                result = requests.post(url, json=header, timeout=10)
                if result.status_code != 200:
                    print("Tx取得失敗で強制終了")
                    sys.exit()

                checker = int(result.json()["result"]["timestampMs"]) // 1000

    digests = [tx["digest"] for tx in digests]
    digests = list(dict.fromkeys(digests))
    return digests, url

async def fetch_tx(url, digests, batch_size=10, sync_count=10):
    sem = asyncio.Semaphore(sync_count)
    async def fetch_batch(session, url, payload):
        async with sem:
            async with session.post(url, json=payload, timeout=70) as response:
                if response.status != 200:
                    raise RuntimeError(f"Tx取得失敗で強制終了 : {response.status}")

                progress.update(1)
                return (await response.json())["result"]

    options = {"showEffects": True, "showEvents": True, "showBalanceChanges": True}
    progress = tqdm(total=(len(digests) // batch_size), leave=False, desc="Tx取得中")
    tasks = []

    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            for i in range(0, len(digests), batch_size):
                batch = digests[i:i+batch_size]
                payload = {"jsonrpc": "2.0", "id": 1, "method": "sui_multiGetTransactionBlocks", "params": [batch, options]}

                task = asyncio.create_task(fetch_batch(session, url, payload))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

    except Exception as e:
        progress.close()
        print(e)
        sys.exit()

    progress.close()
    txs = [tx for batch in results for tx in batch]
    return txs

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
    digests, url = fetch_digest()
    txs          = asyncio.run(fetch_tx(url, digests))

    open("sui/resources/digests.txt", "w").write("\n".join(digests))
    json.dump(txs, open("sui/resources/TxData.json", "w", encoding="utf-8"), indent=2)

    with open("sui/resources/TxData.json", "r", encoding="utf-8") as f:
        txs = json.load(f)
    df = extract_tx(txs)
    df = JST_ticker(df)
    return df

if __name__ == "__main__":
    df = _run_initial()
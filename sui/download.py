import requests, json, time, asyncio, aiohttp, sys

def fetch_digest(url, addresses, cutoff_time=1735657200):     #2025/01/01 00:00:00 JST
    print("Tx一覧取得中...")
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

                for _ in range(5):
                    try:
                        result = requests.post(url, json=header, timeout=60)
                        result.raise_for_status()

                        checker = int(result.json()["result"]["timestampMs"]) // 1000
                        break
                    except requests.RequestException as e: print(f"getTransactionBlockのリトライ error : {e}")
                else:
                    print("getTransactionBlockのリトライ失敗で強制終了")
                    sys.exit()

    digests = [tx["digest"] for tx in digests]
    digests = list(dict.fromkeys(digests))
    return digests


async def fetch_batch(url, session, batch_id, digests):
    options = {"showEffects": True, "showEvents": True, "showBalanceChanges": True}
    payload = {"jsonrpc": "2.0", "id": batch_id, "method": "sui_multiGetTransactionBlocks", "params": [digests, options]}

    start = time.perf_counter()
    async with session.post(url, json=payload) as response:
        elapsed = time.perf_counter() - start
        if response.status != 200:
            print(f"Batch {batch_id} -> {response.status}, {elapsed:.2f}s")
            return None

        data = await response.json()
        print(f"Batch {batch_id} -> {response.status}, {elapsed:.2f}s")
        return data["result"]

async def run_firehose(url, digests, batch_size=50, group_size=20, delay=2):
    connector = aiohttp.TCPConnector(limit=None)
    tasks = []

    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(digests), batch_size):
            batch = digests[i:i+batch_size]
            task = asyncio.create_task(fetch_batch(url, session, i//batch_size, batch))
            tasks.append(task)

            if (i // batch_size + 1) % group_size == 0:
                await asyncio.sleep(delay)

        results = await asyncio.gather(*tasks)

    with open("sui\\resources\\TxData.json", "w", encoding="utf-8") as f:
        json.dump([tx for batch in results for tx in batch ], f, indent=2)

    return results

if __name__ == "__main__":
    with open("sui\\config.json", "r", encoding="utf-8") as f:
        config    = json.load(f)
        api_key   = config["api_key_Chainstack"]
        addresses = config["address"]

    url = f"https://sui-mainnet.core.chainstack.com/{api_key}"

    digests = fetch_digest(url, addresses)
    open("sui\\resources\\digests.txt", "w").write("\n".join(digests))
    digests = [line.strip() for line in open("sui\\resources\\digests.txt")]
    asyncio.run(run_firehose(url, digests))

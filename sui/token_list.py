import requests, json, csv

def token_account():
    with open("sui\\config.json", "r", encoding="shift-jis") as f:
        config    = json.load(f)
        api_key   = config["api_key_Chainstack"]
        addresses = config["address"]

    url = f"https://sui-mainnet.core.chainstack.com/{api_key}"
    for address in addresses:
        cursor, coins = None, []
        while True:
            payload = {"jsonrpc": "2.0", "id": "1", "method": "suix_getAllCoins", "params": [address, cursor]}
            response = requests.post(url, json=payload)

            if response.status_code != 200:
                print(f"トークン一覧取得失敗：{address}")
                continue

            result = response.json()["result"]
            coins.extend([data["coinType"] for data in result["data"]])
            if (cursor := result["nextCursor"]) is None: break

        coins = list(dict.fromkeys(coins))
        coins = ["0x0" + coin[2:] if len(coin.split("::")[0]) == 65 else coin for coin in coins]

        data = [{"address" : "0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI",
                 "decimals": 9, "symbol": "AFSUI"}]
        
        for coin in coins:
            payload = {"jsonrpc": "2.0", "id": "1", "method": "suix_getCoinMetadata", "params": [coin]}
            response = requests.post(url, json=payload)

            if response.status_code != 200:
                print(f"トークンデータ取得失敗：{coin}")
                continue

            if (result := response.json()["result"]) is None: continue
            data.append({"address" : coin, 
                         "decimals": result["decimals"],
                         "symbol"  : result["symbol"].upper()})

        with open(f"sui\\resources\\Token - {address}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["address", "decimals", "symbol"])
            writer.writeheader()
            writer.writerows(data)

if __name__ == "__main__":
    token_account()
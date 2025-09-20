import requests, json, csv

def token_account():
    with open("config.json", "r", encoding="shift-jis") as f:
        config    = json.load(f)
        api_key   = config["api_key_Helius"]
        addresses = config["address"]

    for address in addresses:
        url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        payload = {"jsonrpc": "2.0", "id": "1", "method": "searchAssets",
            "params": {"ownerAddress": address, "tokenType": "fungible", "options": {"showZeroBalance": True }}}

        response = requests.post(url, json=payload).json()
        if not response.get("result", []):
            print(f"トークン一覧取得失敗：{address}")
            continue

        with open(f"data\\Token - {address}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Account", "SPL Address", "decimals", "Symbol"])
            writer.writerow([address, "-", 9, "SOL"])
            
            for res in response["result"]["items"]:
                mint     = res["id"]
                account  = res["token_info"]["associated_token_address"]
                decimals = res["token_info"]["decimals"]
                symbol   = res["content"]["metadata"].get("symbol","").upper()
                writer.writerow([account, mint, decimals, symbol])

if __name__ == "__main__":
    token_account()
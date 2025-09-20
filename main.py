from modules import initial, analyzer, final

def main():
    tx_lists, url = initial.fetch_tx()
    all_txs       = initial.fetch_tx_data(tx_lists, url)  #本番ではTx取得失敗をsys.exitへ変更

    df = initial.extract_tx(all_txs)
    df = initial.filter_scam(df)
    df = initial.JST_ticker(df)
    df = analyzer.analyze(df)
    df = analyzer.copying(df)
    df = final.price(df)

    final.output(df)

if __name__ == "__main__":
    main()

# analyzerの最後部のPOISONIG と def analyze(df)からpoisoningの判定を削除すること！！！
from modules import initial, analyzer, final

def main():
    tx_lists, url = initial.fetch_tx()
    all_txs       = initial.fetch_tx_data(tx_lists, url)

    df = initial.extract_tx(all_txs)
    df = initial.filter_scam(df)
    df = initial.JST_ticker(df)
    df = analyzer.analyze(df)
    df = analyzer.copying(df)
    df = final.price(df)

    final.output(df)

if __name__ == "__main__":
    main()

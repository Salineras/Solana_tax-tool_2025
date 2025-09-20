from resources import initial, analyzer, final
import json

def main():
    # digests = fetch_digest(url, addresses)
    # txs = asyncio.run(run_firehose(url, digests))

    with open("sui\\resources\\TxData.json", "r", encoding="utf-8") as f:
        txs = json.load(f)
    df = initial.extract_tx(txs)
    df = initial.JST_ticker(df)
    df = analyzer.analyze(df)
    df = analyzer.copying(df)

    final.output(df)

if __name__ == "__main__":
    main()
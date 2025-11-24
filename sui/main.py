from resources import initial, analyzer, final
import json, asyncio

def main():
    digests, url = initial.fetch_digest()
    txs = asyncio.run(initial.fetch_tx(url, digests))

    df = initial.extract_tx(txs)
    df = initial.JST_ticker(df)
    df = analyzer.analyze(df)
    df = analyzer.copying(df)

    final.output(df)

if __name__ == "__main__":

    main()

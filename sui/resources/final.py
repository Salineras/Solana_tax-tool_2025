import pandas as pd
import numpy as np

def output(df):
    df_lending = _lending(df)

    df_main = df[df["種類"].notna() | df.index.isin(df[df["コメント"] == "不明"].drop_duplicates("Tx").index)]
    df_main = pd.concat([df_main["Tx"], df_main.loc[:, "日時":]], axis=1)
    df_main = df_main.rename(columns={"価格": "価格（主軸通貨1枚あたりの価格）"})
    df_main["ソース"] = "SUI"

    df_main["sort"] = df_main["コメント"].map({"注意Swap": 1, "不明": 2}).fillna(0)
    df_main = df_main.sort_values(["sort", "日時"]).drop(columns="sort")

    with pd.ExcelWriter("sui\\custom.xlsx", engine="xlsxwriter") as writer:
        df_main   .to_excel(writer, sheet_name="CryptactStandard", index=False)
        df        .to_excel(writer, sheet_name="df", index=False)
        df_lending.to_excel(writer, sheet_name="Lending", index=False, header=False)

        width_settings = {"CryptactStandard": {'B': 19}, "df": {'N': 19},
                          "Lending" : {'B': 19, 'H': 19, 'C': 12, 'I': 12}}

        for sheet, setting in width_settings.items():
            for col, width in setting.items():
                writer.sheets[sheet].set_column(f'{col}:{col}', width)

def _lending(df):
    def process(order, original_df, service):
        columns = ["Tx", "日時", "コメント", "In Amount", "Out Amount", "Ticker"]
        df = original_df.loc[original_df["コメント"].str.contains(service), columns].copy()

        df = df[df["Ticker"] == "USDC"].sort_values("日時").reset_index(drop=True)
        df["USDC"] = df["Out Amount"].fillna(0) - df["In Amount"].fillna(0)

        formula = f"SUM({chr(62 + order * 6)}$3:{chr(62 + order * 6)}"
        df["Earned"] = [f"=IF({formula}{i+3})<0,-{formula}{i+3}),\"\")" for i in range(len(df))]
        df = df.reindex(columns=["Tx", "日時", "コメント", "USDC", "Earned", "space"])

        header = pd.DataFrame([[service] + [None]*5,
                               ["Tx", "JST", "Action", "USDC", "利息計算", None]], columns=df.columns)
        df = pd.concat([header, df], ignore_index=True)
        df = df.rename(columns={col: f"{col}_{service}" for col in df.columns})
        return df

    df_scallop = process(1, df, "Scallop")
    df_kai     = process(2, df, "Kai")

    df = pd.concat([df_scallop, df_kai], axis=1)
    return df


if __name__ == "__main__":
    import initial, analyzer

    df = initial._run_initial()
    df = analyzer.analyze(df)
    df = analyzer.copying(df)

    output(df)
    df.to_csv("sui\\after.csv", index=False, encoding='cp932')

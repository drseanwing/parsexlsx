import pandas as pd

def aggregate_xlsx(df: pd.DataFrame, group_cols: list) -> list:
    agg = df.groupby(group_cols).agg({"URN": "count"}).reset_index()
    agg.rename(columns={"URN": "Count"}, inplace=True)
    return agg.to_dict(orient="records")

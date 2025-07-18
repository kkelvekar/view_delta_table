import pandas as pd
from deltalake import DeltaTable
from tabulate import tabulate

# --- Your Azure Storage Details ---
storage_account_name = "kkdevuksouthordersdl"
container_name       = "raw"

def load_delta_to_df(path, columns=None, safe_cast=False):
    dt = DeltaTable(
        f"az://{container_name}/{path}",
        storage_options={
            "azure_storage_account_name": storage_account_name,
            "use_azure_cli": "true"
        }
    )
    arrow_table = dt.to_pyarrow_table(columns=columns)
    if safe_cast:
        return arrow_table.to_pandas(safe=False)
    return arrow_table.to_pandas()

def truncate_strings(df: pd.DataFrame, max_width: int = 30) -> pd.DataFrame:
    """
    Truncate any string cell in the DataFrame to max_width characters,
    appending '...' if it was longer.
    """
    def _truncate(x):
        if isinstance(x, str) and len(x) > max_width:
            return x[: max_width - 3] + "..."
        return x

    return df.applymap(_truncate)

# 1) Load current products
df_current = load_delta_to_df(
    "products",
    columns=["PRODUCT_ID", "PRODUCT_NAME", "UNIT_PRICE"]
)

# 2) Truncate all string columns to 30 chars
MAX_WIDTH = 30
df_current = truncate_strings(df_current, MAX_WIDTH)

# 3) Print neatly with tabulate
print("\n=== Current Products ===\n")
print(tabulate(df_current, headers="keys", tablefmt="psql", showindex=False))

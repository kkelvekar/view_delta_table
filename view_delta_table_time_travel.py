import os
import argparse
import sys
import polars as pl
from deltalake import DeltaTable
from tabulate import tabulate

# --- Your Azure Storage Details ---
storage_account_name = "kkdevuksouthordersdl"
container_name       = "raw"

def main():
    """
    Interactively list and time-travel a Delta table, authenticating via Azure CLI,
    and read the result using DeltaTable.to_pyarrow_table().
    """
    parser = argparse.ArgumentParser(description="Delta Table Time Travel Viewer")
    parser.add_argument(
        "--path",
        default="products",
        help="Delta table path within container"
    )
    parser.add_argument(
        "--columns",
        nargs="*",
        default=None,
        help="Specific columns to select"
    )
    args = parser.parse_args()

    # 1) Open the table with Azure CLI auth
    dt = DeltaTable(
        f"az://{container_name}/{args.path}",
        storage_options={
            "azure_storage_account_name": storage_account_name,
            "use_azure_cli": "true"
        }
    )

    # 2) Show available versions
    history = dt.history()
    if not history:
        print("No history available for this table.")
        sys.exit(1)

    history_sorted = sorted(history, key=lambda x: x["version"])
    print("\nAvailable versions:")
    for entry in history_sorted:
        ts = entry.get("timestamp", "")
        print(f"  {entry['version']}: {ts}")

    # 3) Ask which version to load
    selection = input("\nEnter version number to load: ").strip()
    try:
        version = int(selection)
    except ValueError:
        print("Invalid version input; must be an integer.")
        sys.exit(1)
    if version not in [e["version"] for e in history_sorted]:
        print(f"Version {version} is not in the available history.")
        sys.exit(1)

    # 4) Time-travel the table
    dt.load_as_version(version)

    # 5) Read the data directly via DeltaTable’s built-in reader
    #    (bypasses fsspec + pyarrow.parquet entirely)
    if args.columns:
        pa_table = dt.to_pyarrow_table(columns=args.columns)
    else:
        pa_table = dt.to_pyarrow_table()

    # 6) Convert to Polars, sort if PRODUCT_ID present
    df = pl.from_arrow(pa_table)
    if "PRODUCT_ID" in df.columns:
        df = df.sort("PRODUCT_ID")

    # 7) Print as a pretty ASCII table
    pdf = df.to_pandas()
    print(f"\n=== Delta table '{args.path}' at version {version} ===\n")
    print(tabulate(pdf, headers="keys", tablefmt="psql", showindex=False))


if __name__ == "__main__":
    main()

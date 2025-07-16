# View Delta Table

This script loads and displays the current products snapshot from an Azure Delta table.

## Prerequisites

- Python 3.8 or newer
- pip package manager

## Installation

1. Clone this repository or download the view_delta_table.py script.
2. Install the required Python packages:

    pip install deltalake pandas tabulate

## Configuration

Open view_delta_table.py and set your Azure Storage details at the top of the file:

    storage_account_name = "<YOUR_ACCOUNT_NAME>"
    storage_account_key  = "<YOUR_ACCOUNT_KEY_OR_SAS_TOKEN>"
    container_name       = "<YOUR_CONTAINER_NAME>"

## Usage

Run the script to fetch and display the current products table:

    python view_delta_table.py

The output will look like:

    === Current Products ===

    +------------+---------------------+-------------+
    | PRODUCT_ID | PRODUCT_NAME        | UNIT_PRICE  |
    |------------+---------------------+-------------|
    | 1001       | Widget A            |        12.5 |
    | 1002       | Gadget B            |         9.99|
    | ...        | ...                 |         ... |
    +------------+---------------------+-------------+
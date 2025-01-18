import pandas as pd
import numpy as np
import datetime as dt
import yfinance as yf
from pathlib import Path
import pytz
import logging

from dataclasses import dataclass

logging.basicConfig(
    filename='./results.log',
    level=logging.INFO,
    filemode='a',
    datefmt='%Y-%m-%d %H:%M:%S',
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')


def _create_contract(row: pd.Series) -> str:
    """
    Generates a contract identifier based on product type and attributes in a row.
    Args:
        row (pd.Series): Row with product details including symbol, currency, and others.
    Returns:
        str: Contract identifier string.
    Example:
        df['contract'] = df.apply(_create_contract, axis=1)
    """
    if row['product_type'] == 'ST':
        contract = '_'.join([row['symbol'].lower(), row['currency'].lower()])
    elif row['product_type'] == 'OP':
        contract = '_'.join([row['symbol'].lower(), row['currency'].lower(),
                             row['maturity_date'].strftime('%Y-%m-%d'), row['put_call'].lower(), str(row['strike'])])
    return contract


def _get_exchange_rate_to_eur(date: dt.datetime | str, col_currency: pd.Series) -> dict[str, float]:
    """
    Retrieves exchange rates for unique currencies to EUR on a specified date.
    Args:
        date (dt.datetime | str): Target date for exchange rates.
        col_currency (pd.Series): Series of currency codes to retrieve rates for.
    Returns:
        dict[str, float]: Mapping of currency-EUR pairs to exchange rates.
    Example:
        dict_rates = _get_exchange_rate_to_eur('2023-10-01', df['currency'])
    """
    currencies = col_currency.unique()
    mapping_rates = {}

    for currency in currencies:
        # Define the currency pairs
        convert_from_x_to_eur = f'EUR{currency}=X'

        # Ensure the date is timezone-aware (using UTC as Yahoo Finance timestamps are UTC)
        target_date = pd.to_datetime(date).tz_localize('UTC')

        # Download historical data
        df_convert_from_x_to_eur = yf.download(tickers=convert_from_x_to_eur, start=target_date, end=target_date + pd.DateOffset(1))

        # Extract the closing values closest to the target date
        rate_to_euro = df_convert_from_x_to_eur['Close'].asof(date) if not df_convert_from_x_to_eur.empty else None
        rate_to_euro = rate_to_euro.values[0]
        currency_key = f'{currency}EUR'
        mapping_rates[currency_key] = rate_to_euro

    return mapping_rates


def read_csvs_from_directory(directory: Path | str) -> dataclass:
    """
    Reads all CSV files in a directory into a dataclass with attributes for each file.
    Args:
        directory (Path | str): Directory containing the CSV files.
    Returns:
        dfs: Dataclass instance with each CSV file as an pd.DataFrame as attribute named by its filename.
    Example:
        dfs = read_csvs_from_directory('data')
        print(dfs.trades_clearing.head())
    """
    logging.info(msg=f'Start function read_csvs_from_directory(directory={directory})')

    # Path object for the directory
    directory_path = Path(directory)

    @dataclass
    class dfs:
        pass

    # List of CSV files found
    csv_files = list(directory_path.glob('*.csv'))

    # Check if any CSV files exist
    if not csv_files:
        logging.info(msg=f'No CSV file was found in the {directory_path} folder.')

    # Iterate through all CSV files in the directory
    else:
        for file_path in csv_files:
            # Use the stem (filename without extension) as the attribute name
            name = f'{file_path.stem}'
            logging.info(msg=f'\tCreated Data Frame: {name}')

            # Read the CSV into a DataFrame
            df = pd.read_csv(file_path)

            # Dynamically set the attribute on the dataclass
            setattr(dfs, name, df)

    # Return an instance of the dataclass
    logging.info(msg=f'Done function read_csvs_from_directory(directory={directory})')
    return dfs()


def create_current_position_desk(dfs: dataclass, TARGET_DATE: str) -> None:
    """
    Initializes and updates the `position_desk` DataFrame in `dfs` with calculated columns for contract,
    price in EUR, and P&L.
    Args:
        dfs (dataclass): Dataclass instance where `position_desk` will be created and updated.
        position_desk_raw (pd.DataFrame): Raw data to copy and convert for trading positions.
        TARGET_DATE (dt.datetime.date | str): Reference date for fetching exchange rates to EUR.
    Modifies:
        - `dfs.position_desk`:
            - Initializes `position_desk` as a copy of `position_trading_raw`.
            - Converts relevant columns to appropriate types.
            - Adds calculated columns:
                - `contract`: Unique identifier for each position based on trading details.
                - `price_eur`: Converted price in EUR, using exchange rates on `TARGET_DATE`.
                - `pnl`: Calculated profit and loss in EUR, based on `price_eur` and `net_position`.
    Example:
        create_current_position_desk(dfs, position_trading_desk, TARGET_DATE='2023-10-01')
    """
    logging.info(msg=f'Start function create_current_position_trading({type(dfs).__name__}, {TARGET_DATE})')

    # Create position_desk attribute
    setattr(dfs, 'position_desk', dfs.raw_trades_desk.copy())
    logging.info(msg='\tCreated dfs.position_desk DataFrame from dfs.position_desk')

    # Convert column types
    dfs.position_desk['maturity_date'] = pd.to_datetime(dfs.position_desk['maturity_date'])
    dfs.position_desk['maturity_date'] = dfs.position_desk['maturity_date'].dt.tz_localize('UTC').dt.tz_convert(
        pytz.timezone('Europe/Amsterdam'))
    cols_numeric = ['strike', 'net_position', 'multiplier']
    dfs.position_desk[cols_numeric] = dfs.position_desk[cols_numeric].apply(pd.to_numeric, errors='coerce')

    # Add columns
    dfs.position_desk['contract'] = dfs.position_desk.apply(_create_contract, axis=1)
    mapping_exchange_rate = _get_exchange_rate_to_eur(date=TARGET_DATE, col_currency=dfs.position_desk['currency'])
    dfs.position_desk['price_eur'] = np.where(
        dfs.position_desk['currency'] == 'EUR',
        dfs.position_desk['price'],
        dfs.position_desk['price'] / dfs.position_desk['currency'].map(lambda x: mapping_exchange_rate.get(f"{x}EUR", 1)))
    dfs.position_desk['pnl'] = dfs.position_desk['price_eur'] * dfs.position_desk['net_position']

    # Define groupby columns
    cols_groupby = ['contract', 'symbol', 'product_type']
    cols_sum = ['net_position', 'pnl']
    cols_agglist = ['price', 'price_eur']

    # Define the aggregation dictionary
    aggregation_dict = {
        **{col: 'sum' for col in cols_sum},  # Sum for numeric columns
        **{col: lambda x: list(x) for col in cols_agglist}  # List for price-related columns
    }

    # Perform the groupby and aggregation
    dfs.position_desk = (
        dfs.position_desk.groupby(cols_groupby)     # Group by specified columns
        .agg(aggregation_dict)     # Apply the aggregation functions
        .reset_index()             # Reset index to make groupby columns part of the DataFrame
    )
    logging.info(msg=f'Done function create_current_position_trading({type(dfs).__name__}, {TARGET_DATE})')


def create_current_position_clearing(dfs: dataclass, TARGET_DATE: str) -> None:
    """
    Initializes and updates the `position_clearing` DataFrame in `dfs` with mappings, converted types, and calculated columns.
    Args:
        dfs (dataclass): Dataclass instance where `position_clearing` will be created and updated.
        position_clearing_raw (pd.DataFrame): Raw data to copy and process for clearing positions.
        TARGET_DATE (str): Reference date for fetching exchange rates to EUR.
    Modifies:
        - `dfs.position_clearing`:
            - Initializes `position_clearing` as a copy of `position_clearing_raw`.
            - Maps `symbol_underlying` to `symbol_trading` from `dfs.mapping_symbol_raw` to create a `symbol` column.
            - Converts relevant columns to appropriate types:
                - `maturity_date` converted to datetime in Amsterdam timezone.
                - Numeric conversion for `strike`, `net_position`, and `multiplier`.
            - Adds calculated columns:
                - `contract`: Unique identifier for each position based on contract details.
                - `price_eur`: Converted price in EUR using exchange rates on `TARGET_DATE`.
                - `pnl`: Calculated profit and loss in EUR, based on `price_eur` and `net_position`.
    Example:
        create_current_position_clearing(dfs, position_clearing_raw, TARGET_DATE='2023-10-01')
    """
    logging.info(msg=f'Done function create_current_position_trading({type(dfs).__name__}, {TARGET_DATE})')

    # Create position_desk attribute
    setattr(dfs, 'position_clearing', dfs.position_clearing_raw.copy())
    logging.info(msg=f'Done function create_current_position_trading({type(dfs).__name__}, {TARGET_DATE})')

    # Create a dictionary mapping from dfs.mapping_symbol_raw
    symbol_mapping = dfs.mapping_symbol_raw.set_index('symbol_clearing')['symbol_trading'].to_dict()

    # Map 'symbol_underlying' in position_clearing to the 'symbol_trading' and create 'symbol'
    dfs.position_clearing['symbol'] = dfs.position_clearing['symbol_underlying'].map(symbol_mapping)

    # Convert column types
    dfs.position_clearing['maturity_date'] = pd.to_datetime(dfs.position_clearing['maturity_date'])
    dfs.position_clearing['maturity_date'] = dfs.position_clearing['maturity_date'].dt.tz_localize('UTC').dt.tz_convert(
        pytz.timezone('Europe/Amsterdam'))
    cols_numeric = ['strike', 'net_position', 'multiplier']
    dfs.position_clearing[cols_numeric] = dfs.position_clearing[cols_numeric].apply(pd.to_numeric, errors='coerce')

    # Add columns
    dfs.position_clearing['contract'] = dfs.position_clearing.apply(_create_contract, axis=1)
    mapping_exchange_rate = _get_exchange_rate_to_eur(date=TARGET_DATE, col_currency=dfs.position_clearing['currency'])
    dfs.position_clearing['price_eur'] = np.where(
        dfs.position_clearing['currency'] == 'EUR',
        dfs.position_clearing['price'],
        dfs.position_clearing['price'] / dfs.position_clearing['currency'].map(lambda x: mapping_exchange_rate.get(f"{x}EUR", 1)))
    dfs.position_clearing['pnl'] = dfs.position_clearing['price_eur'] * dfs.position_clearing['net_position']

    # Define groupby columns
    cols_groupby = ['contract', 'symbol', 'product_type']
    cols_sum = ['net_position', 'pnl']
    cols_agglist = ['price', 'price_eur']

    # Define the aggregation dictionary
    aggregation_dict = {
        **{col: 'sum' for col in cols_sum},  # Sum for numeric columns
        **{col: lambda x: list(x) for col in cols_agglist}  # List for price-related columns
    }

    # Perform the groupby and aggregation
    dfs.position_clearing = (
        dfs.position_clearing.groupby(cols_groupby)     # Group by specified columns
        .agg(aggregation_dict)     # Apply the aggregation functions
        .reset_index()             # Reset index to make groupby columns part of the DataFrame
    )
    logging.info(msg=f'Done function create_current_position_clearing({type(dfs).__name__}, {TARGET_DATE})')


def create_current_position_compared(dfs: dataclass) -> None:
    logging.info(msg=f'Start function create_current_position_compared({type(dfs).__name__}')

    cols_unique = ['contract', 'product_type']
    cols_join = ['contract', 'net_position', 'price', 'price_eur', 'pnl']

    df_unique_contracts = pd.concat([dfs.position_desk[cols_unique], dfs.position_clearing[cols_unique]]).drop_duplicates()

    df_position_current = df_unique_contracts.set_index('contract').join(dfs.position_desk[cols_join].set_index('contract'), how='left')
    df_position_current = df_position_current.join(dfs.position_clearing[cols_join].set_index('contract'), how='left', lsuffix='_trading',
                                                   rsuffix='_clearing')

    df_position_current['is_net_pos_diff'] = df_position_current['net_position_trading'] != df_position_current['net_position_clearing']

    # Drop rows where col1 or col2 is 0 or None
    df_position_current = df_position_current[~((df_position_current['net_position_trading'].isin([0, None])) | (df_position_current['net_position_clearing'].isin([0, None])))]
    setattr(dfs, 'position_compared', df_position_current)
    logging.info(msg='\tCreated dfs.current_position_compared DataFrame from dfs.position_clearing and dfs.position_clearing')
    logging.info(msg=f'Done function create_current_position_compared({type(dfs).__name__}')

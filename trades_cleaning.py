import pandas as pd
import re
import glob
from typing import Dict

# 1. Setup & Helper Functions

from datetime import datetime

def load_and_normalize_csv(filepath: str) -> pd.DataFrame:
    # 1. Read blindly with 20 columns to capture ragged data
    df = pd.read_csv(filepath, header=None, names=range(20))
    # 2. Drop rows and columns that are COMPLETELY empty (all NaN)
    df = df.dropna(axis=0, how='all').dropna(axis=1, how='all') 
    if df.empty:
        raise ValueError(f"File {filepath} contains no valid data rows.")
        
    # 3. Grab the first row, convert Series to strings, apply strip function to every cell in Series
    new_header = df.iloc[0].astype(str).str.strip()
    if "Symbol" not in new_header.values:
        raise ValueError(f"Header Check Failed: First row in {filepath} does not contain 'Symbol'. Row: {new_header.tolist()}")

    # 4. Promote Header
    df = df[1:] # Take data after header
    df.columns = new_header 
    # 5. Clean columns incase "header" row had empty cells that became columns named 'nan' or ''
    cols_to_keep = [c for c in df.columns if c != '' and c != 'nan']
    df = df[cols_to_keep]
    # Reset index
    df = df.reset_index(drop=True) 

    # 6. Filter Aggregate Rows
    df = _filter_aggregate_rows(df)
    
    return df.reset_index(drop=True)

def _filter_aggregate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows that are summary/aggregate lines.
    Criteria: Symbol starts with "Total" AND Code is empty AND Prices are empty.
    """
    # Normalize all columns to str so that we can use string method .startswith() below
    symbol = df['Symbol'].fillna('').astype(str)
    code = df['Code'].fillna('').astype(str)
    t_price = df['T. Price'].fillna('').astype(str).str.strip()
    c_price = df['C. Price'].fillna('').astype(str).str.strip()
    
    # Create Mask
    is_aggregate = (
        symbol.str.startswith('Total') & 
        (code == '') & 
        (t_price == '') & 
        (c_price == '')
    )
    
    return df[~is_aggregate]

def calculate_net_cash(row):
    """
    Calculates Net Cash = Proceeds + Comm/Fee
    Expects operands to be floats. Pre-processing required if they are strings.
    """
    return row['Proceeds'] + row['Comm/Fee']

def assign_buy_sell_action(qty):
    """
    Maps Quantity to Action:
    Negative -> SELL
    Positive -> BUY
    """
    if qty < 0:
        return 'SELL'
    else:
        return 'BUY'

def parse_ibkr_symbol(symbol: str) -> Dict:
    """
    Parses strings like "AMAT 05DEC25 220 C" or "LULU 07NOV25 172.5 P"
    Returns: Symbol, Expiry, Strike, Instrument
    """
    symbol = str(symbol).strip()
    
    # Regex for Options: Ticker + Date + Strike + Right (C/P)
    # Matches: AMAT 05DEC25 220 C
    option_pattern = r"^([A-Z]+)\s+(\d{2}[A-Z]{3}\d{2})\s+([\d\.]+)\s+([CPcp])"
    match = re.search(option_pattern, symbol)
    
    if match:
        # Parse Date: 05DEC25 -> 2025-12-05
        raw_date = match.group(2)
        parsed_date = datetime.strptime(raw_date, "%d%b%y").strftime("%Y-%m-%d")
        
        return {
            'Symbol': match.group(1),
            'Expiry': parsed_date,
            'Strike': float(match.group(3)),
            'Instrument': 'Call Option' if match.group(4).upper() == 'C' else 'Put Option'
        }
    else:
        # Fallback for Stocks (Just the ticker)
        return {
            'Symbol': symbol.split(' ')[0], # Handle cases like "AMAT"
            'Expiry': None,
            'Strike': 0.0,
            'Instrument': 'Stock'
        }

def classify_action(code):
    """
    Translates IBKR codes to simple OPEN/CLOSE ledger actions.
    """
    code = str(code).upper()  
    if 'O' in code: return 'OPEN' # Opening a new position 
    # All these result in closing a position
    if any(x in code for x in ['C', 'A', 'EP', 'EX']): return 'CLOSED'
    return 'UNKNOWN'


# 2. Main Execution
if __name__ == "__main__":
    # 1. Load Data
    all_files = glob.glob('output/ibkr_table-*.csv')
    df_list = []

    print(f"Found {len(all_files)} files to process.")
    for filename in all_files:
        try:
            normalized_df = load_and_normalize_csv(filename)
            df_list.append(normalized_df)
        except Exception as e:
            print(f"Skipping {filename}: {e}")
            
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
    else:
        full_df = pd.DataFrame()

    # 2. Main Processing Pipeline
    if not full_df.empty:
        # Work on a copy
        clean_df = full_df.copy()

        # A. Clean Numeric Columns
        # Remove commas and convert to numeric. Coerce errors to NaN.
        numeric_cols = ['Quantity', 'Proceeds', 'Comm/Fee', 'Strike'] 
        for col in numeric_cols:
            if col in clean_df.columns:
                clean_df[col] = clean_df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')

        # B. Parse Symbol -> Expand to [Symbol, Expiry, Strike, Instrument]
        print("Parsing Symbols...")
        parsed_data = clean_df['Symbol'].apply(parse_ibkr_symbol).apply(pd.Series)
        
        # Drop old Symbol, join new columns
        # We drop 'Symbol' from clean_df first to avoid duplicates, then concat puts new Symbol at front
        clean_df = clean_df.drop(columns=['Symbol'])
        clean_df = pd.concat([parsed_data, clean_df], axis=1)

        clean_df['Status'] = clean_df['Code'].apply(classify_action)
        clean_df['Action'] = clean_df['Quantity'].apply(assign_buy_sell_action)
        clean_df['Net Cash'] = clean_df.apply(calculate_net_cash, axis=1)

        desired_order = [
            'Date/Time', 'Symbol', 'Action', 'Status', 'Quantity', 
            'Net Cash'  , 'Strike', 'Expiry', 'Instrument'
        ]
        final_cols = [c for c in desired_order if c in clean_df.columns]
        final_view = clean_df[final_cols]
        final_view = final_view.rename(columns={'Date/Time': 'Date', 'Quantity': 'Qty'})        
        if 'Date' in final_view.columns:
            final_view['Date'] = pd.to_datetime(final_view['Date'], errors='coerce') # Convert text into Date objects, and bad values into NaT
            final_view = final_view.dropna(subset=['Date']) # Remove any rows where Date became NaT (Not a Time)
            final_view['Date'] = final_view['Date'].dt.date # Drop time component
            final_view = final_view.sort_values(by=['Date', 'Symbol'])

        # E. Save Output
        output_path = 'master_ledger.csv'
        final_view.to_csv(output_path, index=False)
        print(f"\nSuccessfully saved Master Ledger to {output_path}")


    
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
    return df

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
    if any(x in code for x in ['C', 'A', 'EP', 'EX']): return 'CLOSE'
    return 'UNKNOWN'


    
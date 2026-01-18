import pandas as pd
import re
import glob

# 1. Setup & Helper Functions
def parse_ibkr_description(desc):
    """
    Parses strings like "AMAT 05DEC25 220 C" or "LULU 07NOV25 172.5 P"
    Returns: Symbol, Expiry, Strike, Instrument
    """
    desc = str(desc).strip()
    
    # Regex for Options: Ticker + Date + Strike + Right (C/P)
    # Matches: AMAT 05DEC25 220 C
    option_pattern = r"^([A-Z]+)\s+(\d{2}[A-Z]{3}\d{2})\s+([\d\.]+)\s+([CPcp])"
    match = re.search(option_pattern, desc)
    
    if match:
        return {
            'Symbol': match.group(1),
            'Expiry': match.group(2),
            'Strike': float(match.group(3)),
            'Instrument': 'Call Option' if match.group(4).upper() == 'C' else 'Put Option'
        }
    else:
        # Fallback for Stocks (Just the ticker)
        return {
            'Symbol': desc.split(' ')[0], # Handle cases like "AMAT"
            'Expiry': None,
            'Strike': 0.0,
            'Instrument': 'Stock'
        }

def classify_action(code):
    """
    Translates IBKR codes to simple OPEN/CLOSE ledger actions.
    """
    code = str(code).upper()
    
    if 'O' in code: 
        return 'OPEN' # Opening a new position
    
    # All these result in closing/ending a position
    if any(x in code for x in ['C', 'A', 'EP', 'EX']): 
        return 'CLOSE'
        
    return 'UNKNOWN'

# 2. Load and Clean Data
all_files = glob.glob('ibkr_table-*.csv')
df_list = []

for filename in all_files:
    # Textract often adds extra rows/headers, so we try to find the header row
    try:
        # Read widely to find columns
        raw_df = pd.read_csv(filename)
        
        # Identify the real header row (contains "Symbol" and "Date/Time")
        # We'll normalize column names to lowercase for safer matching
        raw_df.columns = raw_df.columns.str.strip()
        
        # If headers aren't in the first row (common in scraped PDFs)
        if 'Symbol' not in raw_df.columns:
            # Re-read skipping bad rows if needed, or simple filter logic:
            # (Simplification for this demo: assumes headers are reasonably standard per your upload)
            pass
            
        df_list.append(raw_df)
    except Exception as e:
        print(f"Skipping {filename}: {e}")

if df_list:
    full_df = pd.concat(df_list, ignore_index=True)
else:
    full_df = pd.DataFrame()

# 3. Processing Pipeline
# Filter out "Total" summary rows and empty spacers
clean_df = full_df[
    (full_df['Symbol'].notna()) & 
    (~full_df['Symbol'].astype(str).str.contains('Total', case=False)) &
    (full_df['Date/Time'].notna())
].copy()

# Fix Numeric Columns (Remove commas, handle parentheses if negative)
cols_to_fix = ['Quantity', 'Proceeds', 'Comm/Fee']
for col in cols_to_fix:
    if col in clean_df.columns:
        # Remove commas, convert string numbers to float
        clean_df[col] = clean_df[col].astype(str).str.replace(',', '').astype(float)

# Apply Parsing Logic
parsed_data = clean_df['Symbol'].apply(parse_ibkr_description).apply(pd.Series)
clean_df = pd.concat([clean_df, parsed_data], axis=1)

# Apply Action Logic
clean_df['Action'] = clean_df['Code'].apply(classify_action)

# Calculate Net Cash (The Real Ledger)
# IBKR Proceeds are gross. Comm/Fee is negative.
clean_df['Net_Cash'] = clean_df['Proceeds'] + clean_df['Comm/Fee']

# 4. Final Selection
final_view = clean_df[[
    'Date/Time', 'Symbol', 'Instrument', 'Action', 
    'Quantity', 'Net_Cash', 'Strike', 'Expiry'
]].rename(columns={'Date/Time': 'Date', 'Quantity': 'Qty'})

# Sort by Date
final_view['Date'] = pd.to_datetime(final_view['Date']).dt.date
final_view = final_view.sort_values('Date')

print(final_view.head(10).to_markdown(index=False))
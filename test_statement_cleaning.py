import pytest
import pandas as pd
import glob
import os
from io import StringIO
from trades_cleaning import classify_action, parse_ibkr_description, load_and_normalize_csv

# ==========================================
# 1. Existing Tests (Legacy)
# ==========================================

@pytest.mark.parametrize("input_code, expected_action", [
    ("O", "OPEN"),
    ("o", "OPEN"),
    ("C", "CLOSE"),
    ("A;C", "CLOSE"),
    ("Ep", "CLOSE"),
    (" ", "UNKNOWN"),
])
def test_classify_action(input_code, expected_action):
    assert classify_action(input_code) == expected_action

# Note: I renamed the function to match the one imported from trades_cleaning
@pytest.mark.parametrize("input_string, expected_symbol, expected_strike, expected_expiry, expected_instrument", [
    ("AMAT 05DEC25 220 C", "AMAT", 220.0, "2025-12-05", "Call Option"),
    ("LULU 07NOV25 175 P", "LULU", 175.0, "2025-11-07", "Put Option"),
    ("AMAT", "AMAT", 0.0, None, "Stock"),
])
def test_parse_ibkr_symbol(input_string, expected_symbol, expected_strike, expected_expiry, expected_instrument):
    result = parse_ibkr_description(input_string)
    assert result['Symbol'] == expected_symbol
    assert result['Strike'] == expected_strike
    assert result['Expiry'] == expected_expiry
    assert result['Instrument'] == expected_instrument

# ==========================================
# 3. Real File Tests (Integration Tests)
# ==========================================

# Dynamically find all CSV files in the output directory
REAL_CSV_FILES = glob.glob(os.path.join("output", "ibkr_table-*.csv"))

@pytest.mark.parametrize("filepath", REAL_CSV_FILES)
def test_load_and_normalize_real_csvs(filepath):
    """
    Runs the loading logic against every real file in output/ 
    to ensure it works on actual data.
    """
    print(f"Testing real file: {filepath}")
    
    # 1. Run the function
    df = load_and_normalize_csv(filepath)
    
    # 2. Basic Validity Checks
    assert not df.empty, f"File {filepath} resulted in an empty dataframe"
    
    # Check for critical columns
    # We allow some variation, but Symbol and Date/Time are non-negotiable for our logic
    assert "Symbol" in df.columns, f"Missing 'Symbol' column in {filepath}"
    assert "Date/Time" in df.columns, f"Missing 'Date/Time' column in {filepath}"
    
    # Check that we didn't accidentally include the 'Realized' header row as data 
    # (A common bug where the header isn't promoted correctly)
    first_symbol = str(df.iloc[0]['Symbol'])
    assert first_symbol != "Symbol", f"Header row was not correctly promoted in {filepath}"

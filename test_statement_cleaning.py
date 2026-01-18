import pytest
import pandas as pd
from trades_cleaning import classify_action

@pytest.mark.parametrize("input_code, expected_action", [
    # Each tuple represents one run of the test
    ("O", "OPEN"),          # Standard Open
    ("o", "OPEN"),          # Lowercase handling
    ("C", "CLOSE"),         # Standard Close
    ("A;C", "CLOSE"),       # Assignment is a Close
    ("Ep", "CLOSE"),        # Expired Position is a Close
    (" ", "UNKNOWN"),       # Empty value
])
def test_classify_action(input_code, expected_action):
    assert classify_action(input_code) == expected_action

@pytest.mark.parametrize("input_string, expected_symbol, expected_strike, expected_expiry, expected_instrument", [
    # The Rows of Data
    ("AMAT 05DEC25 220 C", "AMAT", 220.0, "2025-12-05", "Call Option"),  # Run 1
    ("LULU 07NOV25 175 P", "LULU", 175.0, "2025-11-07", "Put Option"),  # Run 2
    ("AMAT", "AMAT", 0.0, None, "Stock"),  # Run 3
])
def test_parse_ibkr_symbol(input_string, expected_symbol, expected_strike, expected_expiry, expected_instrument):
    result = parse_ibkr_symbol(input_string)
    assert result['Symbol'] == expected_symbol
    assert result['Strike'] == expected_strike
    assert result['Expiry'] == expected_expiry
    assert result['Instrument'] == expected_instrument


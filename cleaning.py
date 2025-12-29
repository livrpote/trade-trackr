import pandas as pd
from typing import List, Dict

class StatementCleaner:
    """
    Encapsulates logic for cleaning IBKR CSV statements.
    
    Why a class?
    - State Management: We store the dataframe in `self.df`. We don't need to pass 
      variables (like df) in and out of every function.
    - Encapsulation: The complex logic of HOW we clean is hidden inside methods.
      The user of this class just calls `clean()`.
    """
    def __init__(self, file_path: str):
        # Load the data immediately upon creation.
        # header=0 tells Pandas the first row (index 0) contains the initial messy column names.
        self.df = pd.read_csv(file_path, header=0)
        self.summaries: Dict[str, str] = {} # Store extracted totals here
        print(f"--- Loaded Initial DataFrame {file_path} ---\n{self.df.head(10)}")

    def process(self) -> pd.DataFrame:
        """Orchestrates the cleaning process."""
        self._filter_realized_columns()
        self._promote_header()
        
        # Extract summary stats before we filter out rows
        self._extract_summaries()
        
        # Clean out metadata and summary rows
        self._remove_metadata_rows()
        
        self._keep_target_columns(['Symbol', 'Total'])
        return self.df

    def _filter_realized_columns(self):
        """
        Finds 'Unrealized' and drops it and everything to the right.
        
        Efficiency:
        - get_loc(): O(N) search to find the index integer. Fast.
        - iloc[]: Slicing by integer position. This creates a 'view' on the data
          (shared memory) rather than copying it immediately, which saves memory.
        """
        try:
            # Find the integer index of the column named 'Unrealized'
            cutoff_idx = self.df.columns.get_loc('Unrealized')
            
            # Slice: Keep all rows (:), and columns up to cutoff_idx (:cutoff_idx)
            # This excludes 'Unrealized' and everything after it.
            self.df = self.df.iloc[:, :cutoff_idx]
            
        except KeyError:
            raise ValueError(f"'Unrealized' column not found in {self.df.columns}")

    def _promote_header(self):
        """
        Makes the first row (Index 0) the new header and removes it from data.
        """
        # 1. Grab the first row (which contains 'Symbol', 'Cost Adj', etc.)
        new_header = self.df.iloc[0]
        
        # 2. Slice the dataframe to skip the first row (take row 1 onwards)
        self.df = self.df[1:]
        
        # 3. Assign the new header
        self.df.columns = new_header
        
        # Optional: Reset the index so it starts at 0 again, cleaner for debugging
        self.df.reset_index(drop=True, inplace=True)

    def _extract_summaries(self):
        """
        Scans for specific summary rows and extracts their 'Total' value.
        """
        targets = [
            "Total Stocks", 
            "Total Equity and Index Options", 
            "Total (All Assets)",
            "Total (Combined Assets)"
        ]
        
        if 'Symbol' in self.df.columns and 'Total' in self.df.columns:
            for target in targets:
                # Find the row where Symbol matches target
                match = self.df[self.df['Symbol'] == target]
                
                if not match.empty:
                    # Get the value from the 'Total' column of the first match
                    # We store it as a string initially (it has commas like '10,631.40')
                    raw_val = match.iloc[0]['Total']
                    self.summaries[target] = raw_val

    def _remove_metadata_rows(self):
        """
        Removes metadata rows AND summary rows to leave only raw transactions.
        """
        # 1. Filter out rows with empty Totals (metadata headers)
        if 'Total' in self.df.columns:
            # 1. Create the mask (condition)
            has_valid_total = self.df['Total'].notna()
            # 2. Apply the boolean mask
            self.df = self.df[has_valid_total]
        
        # 2. Filter out explicit metadata and summary rows
        if 'Symbol' in self.df.columns:
            # Convert to string to avoid errors if there are mixed types
            symbol_col = self.df['Symbol'].astype(str)
            
            # Create masks for things we want to REMOVE
            is_carried_by = symbol_col.str.startswith('Carried by')
            is_total_row = symbol_col.str.startswith('Total ')
            is_stocks_header = (symbol_col == 'Stocks')
            is_crypto_header = (symbol_col == 'Crypto')
            is_forex_header = (symbol_col == 'Forex')
            is_currency = (symbol_col == 'GBP')
            is_crypto = (symbol_col == 'BTC.USD-PAXOS')
            
            # Combine masks: We want to KEEP rows that match NONE of these
            # ~(A | B | C) is equivalent to (Not A) & (Not B) & (Not C)
            mask_to_remove = is_carried_by | is_total_row | is_stocks_header | is_crypto_header | is_forex_header | is_currency | is_crypto
            
            self.df = self.df[~mask_to_remove]

    def _keep_target_columns(self, target_cols: List[str]):
        """
        Keeps only columns present in the target_cols list.
        """
        # List comprehension to find columns that match our targets.
        cols_to_keep = [col for col in self.df.columns if col in target_cols]
        
        # Update self.df to only contain these columns
        self.df = self.df[cols_to_keep]

if __name__ == "__main__":
    # Example: Processing two files and merging them
    files = ["output/ibkr_table-1.csv", "output/ibkr_table-2.csv"]
    all_dfs = []
    all_summaries = {}
    
    print("--- Processing Files ---")
    for f in files:
        print(f"Cleaning {f}...")
        cleaner = StatementCleaner(f)
        df = cleaner.process()
        
        # Collect results
        all_dfs.append(df)
        all_summaries.update(cleaner.summaries)
    
    # Merge the dataframes
    final_combined_df = pd.concat(all_dfs, ignore_index=True)
    
    print("\n--- Extracted Summaries ---")
    for k, v in all_summaries.items():
        print(f"{k}: {v}")
        
    print("\n--- Final Combined DataFrame (First 15 Rows) ---")
    print(final_combined_df)
    print(f"\nTotal Rows: {len(final_combined_df)}")

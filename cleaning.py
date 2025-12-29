import pandas as pd
from typing import List

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
        # We load the data immediately upon creation.
        # header=0 tells Pandas the first row (index 0) contains the initial messy column names.
        self.df = pd.read_csv(file_path, header=0)

    def process(self) -> pd.DataFrame:
        """Orchestrates the cleaning process."""
        self._filter_realized_columns()
        self._promote_header()
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
            print("Warning: 'Unrealized' column not found. Skipping slice.")

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

    def _keep_target_columns(self, target_cols: List[str]):
        """
        Keeps only columns present in the target_cols list.
        """
        # List comprehension to find columns that match our targets.
        # We use strict matching here.
        cols_to_keep = [col for col in self.df.columns if col in target_cols]
        
        # Update self.df to only contain these columns
        self.df = self.df[cols_to_keep]

if __name__ == "__main__":
    # Usage Example
    cleaner = StatementCleaner("output/ibkr_table-1.csv")
    final_df = cleaner.process()
    
    print("--- Final Columns ---")
    print(final_df.columns)
    print("\n--- First 5 Rows ---")
    print(final_df.head(10))

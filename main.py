"""
Complete IBKR Statement Parser using AWS Textract
- Uploads PDF to S3 bucket
- Calls Textract async API
- Handles pagination
- Generates one CSV file per table + JSON file containing raw Textract response
"""

from aws_textract_table_extractor import TextractTableExtractor
import pandas

# Configuration
BUCKET = 'ibkr-statements'
PDF_PATH = '/Users/personal/Desktop/Projects/ibkr-statement-parser/IBKR_Personal_Realised_P&L_FY2025.pdf'
OUTPUT_PREFIX = 'output/ibkr_table'

# Extract tables
extractor = TextractTableExtractor(region_name='us-east-2')

try:
    output_files = extractor.extract_tables_from_pdf(
        pdf_path=PDF_PATH,
        bucket=BUCKET,
        output_prefix=OUTPUT_PREFIX
    )
    
    print(f"\n{'-'*80}✅ SUCCESS{'-'*80}")
    print(f"Generated {len(output_files)} CSV files:")
    for f in output_files:
        print(f"  • {f}")
    
except Exception as e:
    print("\n" + "="*80)
    print("❌ ERROR")
    print("="*80)
    print(f"{type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# Read tables into Pandas DataFrames

## Cleaning to get Realized P&L Totals

# 1. Find column index with header "Unrealized" and drop all columns including & after it

# 2. Check if 1st row only contains 1 non-blank value "Realized" and if yes drop row

# 3. Find column index with Header "Symbol" and "Total", and drop all other columns


# 3. In column 0, find row index with value "Stocks" -> start index of stocks table
# 4. Find row index with value "Total Stocks" -> end index of stocks table
# 5. Extract stocks table as separate DataFrame

# 6. Find row index with value "Equity and Index Options"
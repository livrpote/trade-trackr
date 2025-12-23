"""
Complete IBKR Statement Parser using AWS Textract
- Uploads PDF to S3 bucket
- Calls Textract async API
- Handles pagination
- Generates one CSV file per table + JSON file containing raw Textract response
"""

from aws_textract_table_extractor import TextractTableExtractor

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
    
    print("\n" + "="*80)
    print("✅ SUCCESS")
    print("="*80)
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

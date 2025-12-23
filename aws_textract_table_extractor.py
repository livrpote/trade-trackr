import boto3
import time
import json
import pandas as pd
from typing import List, Dict


class TextractTableExtractor:
    """Extract tables from PDFs using AWS Textract."""
    
    def __init__(self, region_name='us-east-2'):
        self.textract = boto3.client('textract', region_name=region_name)
        self.s3 = boto3.client('s3', region_name=region_name)
        self.region = region_name
    
    def extract_tables_from_pdf(self, pdf_path: str, bucket: str, output_prefix: str = 'table') -> List[str]:
        """
        Extract tables from PDF and save as CSV files.
        
        Args:
            pdf_path: Local path to PDF file
            bucket: S3 bucket name
            output_prefix: Prefix for output CSV files
            
        Returns:
            List of generated CSV filenames
        """
        # Step 1: Upload to S3
        s3_key = 'temp_textract_upload.pdf'
        print(f"📤 Uploading {pdf_path} to S3...")
        self.s3.upload_file(pdf_path, bucket, s3_key)
        print(f"✓ Uploaded to s3://{bucket}/{s3_key}")
        
        try:
            # Step 2: Start Textract job
            print("\n🔍 Starting Textract analysis...")
            response = self.textract.start_document_analysis(
                DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': s3_key}},
                FeatureTypes=['TABLES']
            )
            job_id = response['JobId']
            print(f"✓ Job started: {job_id}")
            
            # Step 3: Wait for completion
            all_blocks = self._wait_and_get_results(job_id)
            
            # Step 4: Save response for debugging (optional)
            with open('api_response.json', 'w') as f:
                json.dump({'Blocks': all_blocks}, f, indent=2)
            print(f"✓ Saved raw response to api_response.json")
            
            # Step 5: Generate CSV files
            output_files = self._generate_csv_files(all_blocks, output_prefix)
            
            return output_files
            
        finally:
            # Cleanup S3
            print("\n🧹 Cleaning up S3...")
            self.s3.delete_object(Bucket=bucket, Key=s3_key)
            print("✓ Deleted temporary S3 file")
    
    def _wait_and_get_results(self, job_id: str) -> List[Dict]:
        """Wait for job completion and retrieve all results with pagination."""
        print("\n⏳ Waiting for job to complete...")
        
        # Poll for completion
        while True:
            result = self.textract.get_document_analysis(JobId=job_id)
            status = result['JobStatus']
            
            if status == 'SUCCEEDED':
                print(f"✓ Job completed successfully")
                break
            elif status == 'FAILED':
                error_msg = result.get('StatusMessage', 'Unknown error')
                raise Exception(f"Textract job failed: {error_msg}")
            elif status in ['IN_PROGRESS', 'PENDING']:
                print(f"  Status: {status}...")
                time.sleep(3)
            else:
                raise Exception(f"Unexpected status: {status}")
        
        # Collect all blocks (handle pagination)
        print("\n📥 Retrieving results...")
        all_blocks = []
        next_token = None
        page_count = 0
        
        while True:
            page_count += 1
            if next_token:
                result = self.textract.get_document_analysis(
                    JobId=job_id,
                    NextToken=next_token
                )
            else:
                result = self.textract.get_document_analysis(JobId=job_id)
            
            all_blocks.extend(result['Blocks'])
            next_token = result.get('NextToken')
            
            print(f"  Page {page_count}: Retrieved {len(result['Blocks'])} blocks")
            
            if not next_token:
                break
        
        print(f"✓ Total blocks retrieved: {len(all_blocks)}")
        return all_blocks
    
    def _generate_csv_files(self, blocks: List[Dict], output_prefix: str) -> List[str]:
        """Generate CSV files from Textract blocks."""
        print("\n📊 Processing tables...")
        
        # Build blocks map
        blocks_map = {block['Id']: block for block in blocks}
        
        # Find all TABLE blocks
        table_blocks = [b for b in blocks if b['BlockType'] == 'TABLE']
        print(f"✓ Found {len(table_blocks)} tables")
        
        if not table_blocks:
            print("⚠️  No tables found in document")
            return []
        
        # Process each table
        output_files = []
        for idx, table_block in enumerate(table_blocks, start=1):
            print(f"\n  Processing Table {idx}...")
            
            # Convert to DataFrame
            df = self._table_to_dataframe(table_block, blocks_map)
            
            if df is None:
                print(f"    ✗ Could not process Table {idx}")
                continue
            
            print(f"    Dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
            
            # Save to CSV
            output_file = f"{output_prefix}-{idx}.csv"
            self._save_table_as_csv(df, output_file)
            output_files.append(output_file)
        
        return output_files
    
    def _table_to_dataframe(self, table_block: Dict, blocks_map: Dict) -> pd.DataFrame:
        """Convert a Textract TABLE block to pandas DataFrame."""
        # Get all CELL blocks that are children of this table
        cell_blocks = []
        
        for relationship in table_block.get('Relationships', []):
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    cell = blocks_map.get(child_id)
                    if cell and cell['BlockType'] == 'CELL':
                        cell_blocks.append(cell)
        
        if not cell_blocks:
            return None
        
        # Find table dimensions
        max_row = max(cell['RowIndex'] for cell in cell_blocks)
        max_col = max(cell['ColumnIndex'] for cell in cell_blocks)
        
        # Initialize empty grid
        grid = [['' for _ in range(max_col)] for _ in range(max_row)]
        
        # Fill the grid
        for cell in cell_blocks:
            row_idx = cell['RowIndex'] - 1  # Convert to 0-based
            col_idx = cell['ColumnIndex'] - 1
            
            # Handle merged cells
            row_span = cell.get('RowSpan', 1)
            col_span = cell.get('ColumnSpan', 1)
            
            # Extract text from cell
            text = self._get_text_from_cell(cell, blocks_map)
            
            # Fill all cells covered by this cell (for merged cells)
            for r in range(row_span):
                for c in range(col_span):
                    target_row = row_idx + r
                    target_col = col_idx + c
                    if target_row < max_row and target_col < max_col:
                        # Only fill if empty (first occurrence wins)
                        if grid[target_row][target_col] == '':
                            grid[target_row][target_col] = text
        
        return pd.DataFrame(grid)
    
    def _get_text_from_cell(self, cell: Dict, blocks_map: Dict) -> str:
        """Extract text from a cell block."""
        text_parts = []
        
        if 'Relationships' not in cell:
            return ''
        
        for relationship in cell['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map.get(child_id)
                    if word and word['BlockType'] == 'WORD':
                        text_parts.append(word['Text'])
        
        return ' '.join(text_parts)
    
    def _save_table_as_csv(self, df: pd.DataFrame, filename: str):
        """Save DataFrame as CSV matching AWS UI format."""
        # Add ' prefix to all cells to match UI format
        df_quoted = df.map(lambda x: f"'{x}")
        
        # Save with proper quoting
        df_quoted.to_csv(
            filename,
            index=False,
            header=False,
            quoting=1,  # QUOTE_ALL
        )
        
        print(f"    ✓ Saved {filename}")


import boto3
import time

textract = boto3.client('textract', region_name='us-east-2')
s3 = boto3.client('s3', region_name='us-east-2')

BUCKET = 'ibkr-statements'
PDF = '/Users/personal/Desktop/Projects/ibkr-statement-parser/IBKR_Personal_Realised_P&L_FY2025.pdf'

# Upload and analyze
s3.upload_file(PDF, BUCKET, 'temp.pdf')
print("Uploaded PDF")

job = textract.start_document_analysis(
    DocumentLocation={'S3Object': {'Bucket': BUCKET, 'Name': 'temp.pdf'}},
    FeatureTypes=['TABLES']
)
print(f"Started job: {job['JobId']}")

# Wait for completion
while True:
    result = textract.get_document_analysis(JobId=job['JobId'])
    status = result['JobStatus']
    print(f"Status: {status}")
    if status == 'SUCCEEDED':
        break
    elif status == 'FAILED':
        raise RuntimeError(f"Textract job failed: {result.get('StatusMessage', 'Unknown error occurred retrieving document analysis')}")
    time.sleep(2)

# Collect ALL blocks across all pages
all_blocks = []
next_token = None

while True:
    if next_token:
        result = textract.get_document_analysis(
            JobId=job['JobId'],
            NextToken=next_token
        )
    else:
        result = textract.get_document_analysis(JobId=job['JobId'])
    
    all_blocks.extend(result['Blocks'])
    
    next_token = result.get('NextToken')
    if not next_token:
        break
    
    print(f"Retrieved {len(all_blocks)} blocks so far...")

print(f"\nPages Found: {result['DocumentMetadata']['Pages']}")
print(f"Total Blocks Retrieved: {len(all_blocks)}")

# Find tables
tables = [b for b in all_blocks if b['BlockType'] == 'TABLE']
print(f"✓ Found {len(tables)} tables")

for block in all_blocks:
    if block['BlockType'] == 'LINE' AND block['Text'] == 'AMAT':

# Cleanup
s3.delete_object(Bucket=BUCKET, Key='temp.pdf')
print("Cleaned up")
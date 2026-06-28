import csv

# Check which columns have data
with open('akatsi_district_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    row = next(reader)
    
    print('=== Checking all BECE-related columns ===')
    for key in sorted(row.keys()):
        if 'BECE' in key or 'Final' in key or 'Predicted' in key:
            val = row.get(key, '')
            print(f'{key}: "{val}"')
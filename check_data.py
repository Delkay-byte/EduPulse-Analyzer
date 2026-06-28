import csv

# Check what Official_Results_Raw contains
with open('akatsi_district_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    print('=== Checking Official_Results_Raw column ===')
    for i, row in enumerate(reader):
        if i < 10:
            official = row.get('Official_Results_Raw', '')
            print(f'Row {i}: Official_Results_Raw = "{official}"')
        else:
            break

# Also check a few Final_BECE values
print('\n=== Sample Final_BECE scores ===')
with open('akatsi_district_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        if i < 3:
            print(f"{row['Student_Name']} ({row['School_Name']}):")
            print(f"  Math: {row.get('Mathematics_Final_BECE', '')}")
            print(f"  English: {row.get('English_Language_Final_BECE', '')}")
            print(f"  Science: {row.get('Integrated_Science_Final_BECE', '')}")
            print(f"  Social: {row.get('Social_Studies_Final_BECE', '')}")
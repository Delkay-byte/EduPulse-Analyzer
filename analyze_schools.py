import csv
from collections import defaultdict
import statistics

# Read the CSV data
schools_data = defaultdict(list)
with open('akatsi_district_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        school = row['School_Name']
        
        # Get final BECE scores for all subjects
        subjects = [
            'Mathematics_Final_BECE',
            'English_Language_Final_BECE',
            'Integrated_Science_Final_BECE',
            'Social_Studies_Final_BECE',
            'ICT_Final_BECE',
            'RME_Final_BECE',
            'BDT_Final_BECE',
            'Creative_Arts_Final_BECE',
            'French_Final_BECE',
            'Arabic_Final_BECE',
            'Ewe_Final_BECE'
        ]
        
        scores = []
        for subj in subjects:
            val = row.get(subj, '').strip()
            if val and val != '':
                try:
                    scores.append(float(val))
                except:
                    pass
        
        if len(scores) >= 6:
            # Best 6 aggregate (lower is better)
            best_6 = sorted(scores)[:6]
            best_6_agg = sum(best_6)
            schools_data[school].append(best_6_agg)

# Calculate school statistics
school_stats = []
for school, aggregates in schools_data.items():
    if aggregates:
        avg_agg = statistics.mean(aggregates)
        num_students = len(aggregates)
        school_stats.append({
            'name': school,
            'avg_aggregate': avg_agg,
            'students': num_students,
            'circuit': ''  # Will fill from first student
        })

# Get circuit info
with open('akatsi_district_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        school = row['School_Name']
        if school and any(s['name'] == school for s in school_stats):
            for s in school_stats:
                if s['name'] == school and not s['circuit']:
                    s['circuit'] = row['Circuit']
                    break

# Sort by average aggregate (lower is better)
school_stats.sort(key=lambda x: x['avg_aggregate'])

print('=== SCHOOL RANKINGS (Best 6 Aggregate) ===')
for i, school in enumerate(school_stats, 1):
    print(f"{i}. {school['name']} ({school['circuit']}): Avg {school['avg_aggregate']:.2f} ({school['students']} students)")

print(f'\nTotal schools with data: {len(school_stats)}')
print(f'Total students: {sum(s["students"] for s in school_stats)}')

# Calculate district-wide stats
all_aggregates = []
for aggregates in schools_data.values():
    all_aggregates.extend(aggregates)

print(f'\n=== DISTRICT STATS ===')
print(f'Total students with scores: {len(all_aggregates)}')
print(f'District average aggregate: {statistics.mean(all_aggregates):.2f}')
print(f'Median aggregate: {statistics.median(all_aggregates):.2f}')

# Count by circuit
circuit_counts = defaultdict(int)
for school in school_stats:
    circuit_counts[school['circuit']] += school['students']

print(f'\n=== BY CIRCUIT ===')
for circuit, count in sorted(circuit_counts.items()):
    print(f'{circuit}: {count} students')
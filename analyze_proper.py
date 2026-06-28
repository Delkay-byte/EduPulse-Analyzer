import csv
from collections import defaultdict
import statistics

# Read the CSV data - use Predicted_BECE as the actual BECE scores
schools_data = defaultdict(list)
school_circuits = {}

with open('akatsi_district_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        school = row['School_Name']
        circuit = row['Circuit']
        
        # Store circuit info
        if school and school not in school_circuits:
            school_circuits[school] = circuit
        
        # Get Predicted BECE scores (these appear to be the actual scores)
        subjects = [
            'Mathematics_Predicted_BECE',
            'English_Language_Predicted_BECE',
            'Integrated_Science_Predicted_BECE',
            'Social_Studies_Predicted_BECE',
            'ICT_Predicted_BECE',
            'RME_Predicted_BECE',
            'BDT_Predicted_BECE',
            'Creative_Arts_Predicted_BECE',
            'French_Predicted_BECE',
            'Arabic_Predicted_BECE',
            'Ewe_Predicted_BECE'
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
            'circuit': school_circuits.get(school, 'Unknown')
        })

# Sort by average aggregate (lower is better)
school_stats.sort(key=lambda x: x['avg_aggregate'])

print('=== AKATSI MUNICIPAL SCHOOL RANKINGS ===')
print('Ranking by Average Best 6 Aggregate (Lower is Better)\n')
for i, school in enumerate(school_stats, 1):
    print(f"{i}. {school['name']}")
    print(f"   Circuit: {school['circuit']}")
    print(f"   Avg Best 6 Aggregate: {school['avg_aggregate']:.2f}")
    print(f"   Students with Data: {school['students']}")
    print()

print(f'Total schools with complete data: {len(school_stats)}')
print(f'Total students analyzed: {sum(s["students"] for s in school_stats)}')

# Calculate district-wide stats
all_aggregates = []
for aggregates in schools_data.values():
    all_aggregates.extend(aggregates)

print(f'\n=== DISTRICT SUMMARY ===')
print(f'Total student records: {len(all_aggregates)}')
print(f'District average aggregate: {statistics.mean(all_aggregates):.2f}')
print(f'Median aggregate: {statistics.median(all_aggregates):.2f}')
print(f'Best aggregate: {min(all_aggregates):.2f}')
print(f'Worst aggregate: {max(all_aggregates):.2f}')

# Count by circuit
circuit_counts = defaultdict(int)
circuit_schools = defaultdict(list)
for school in school_stats:
    circuit_counts[school['circuit']] += school['students']
    circuit_schools[school['circuit']].append(school['name'])

print(f'\n=== BY CIRCUIT ===')
for circuit in sorted(circuit_counts.keys()):
    print(f'{circuit}:')
    print(f'  Students: {circuit_counts[circuit]}')
    print(f'  Schools: {len(circuit_schools[circuit])}')
    for s in circuit_schools[circuit]:
        print(f'    - {s}')
    print()
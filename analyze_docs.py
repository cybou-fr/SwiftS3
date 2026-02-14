import os
import re

def analyze_file(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    func_lines = []
    for i, line in enumerate(lines):
        if re.match(r'^\s*(public\s+|private\s+|internal\s+|fileprivate\s+)?(static\s+)?(func\s+\w+|init\s*\()', line):
            func_lines.append(i)
    
    total = len(func_lines)
    documented = 0
    for line_num in func_lines:
        # Check previous 10 lines for ///
        start = max(0, line_num - 10)
        prev_lines = lines[start:line_num]
        if any('///' in line for line in prev_lines):
            documented += 1
    
    return total, documented

total_funcs = 0
total_documented = 0

for root, dirs, files in os.walk('/Users/cybou/Documents/SwiftS3/Sources/SwiftS3'):
    for file in files:
        if file.endswith('.swift'):
            filepath = os.path.join(root, file)
            funcs, docs = analyze_file(filepath)
            total_funcs += funcs
            total_documented += docs
            coverage = (docs / funcs * 100) if funcs > 0 else 0
            print(f'{os.path.basename(filepath)}: {docs}/{funcs} ({coverage:.1f}%)')

overall_coverage = (total_documented / total_funcs * 100) if total_funcs > 0 else 0
print(f'\nOverall: {total_documented}/{total_funcs} ({overall_coverage:.1f}%)')
import sys

print("Cloud based applications- assignment1")
print("="*50)

# Part1
print("\nRunning Spark Analysis...")
exec(open("./spark_analysis.py").read())

# Part2
print("\nRunning MongoDB Operations...")
exec(open("./mongo_operations.py").read())

print("="*50)
print("="*50)
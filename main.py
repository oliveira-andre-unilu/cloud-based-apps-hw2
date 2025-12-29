"""
Author: Andre Martins (ID:0230991223)

Script responsible for running both scripts and saving the output into the respective file.
"""
import sys
from datetime import datetime

timestamp : str = '{date:%Y-%m-%d_%H-%M-%S}'.format( date=datetime.now() )
log_file = open(f"output/execution_output_{timestamp}.txt", 'w')

print("Cloud based applications- assignment1")
print("="*50)

class Tee:
    """Class allowing to log print the output as well as saving it into a output file"""
    def __init__(self, *files):
        self.files = files
    
    def write(self, obj):
        for f in self.files:
            f.write(obj)
    
    def flush(self):
        for f in self.files:
            f.flush()

sys.stdout = Tee(sys.stdout, log_file)


# Part1
print("\nRunning Spark Analysis...")
exec(open("./spark_analysis.py").read())

# Part2
print("\nRunning MongoDB Operations...")
exec(open("./mongo_operations.py").read())

print("="*50)
print("="*50)

log_file.close()
sys.stdout = sys.__stdout__
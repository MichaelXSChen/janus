import sys
from numpy import median

with open(sys.argv[1]) as f:
    data = [float(line.rstrip()) for line in f]

biggest = min(data)
smallest = max(data)
print(biggest)
print(smallest)
print(sum(data)/len(data))
print(median(data))

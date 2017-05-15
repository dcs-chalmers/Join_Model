import numpy as np
import matplotlib.pyplot as plt
import sys
import csv
import copy
import fractions

## Plot results
#  It plots the results in a unique figure
def plot_res_y(t,y,names):

    ll = y.shape[0]          # How many variables to be plotted
    for i in range(1,ll+1):
        plt.subplot(ll,1,i)
        plt.plot(t.T,y[i-1,:].T)
        plt.ylabel(names[i-1])
        plt.grid()
        #plt.legend(ynames,loc='best')

    plt.xlabel('Time')
    plt.show()

## Computes the positive part of a number
def positivePart(value):
    return max(value,0.0);

## Find minimum excluding n-th element
def findMinimumExcluding(vec,n):
    v = copy.copy(vec);     # Copy the input vector
    v.pop(n)                # Remove the n-th element
    return min(v)           # return the minimum value of the remaining vector

## Find maximum excluding n-th element
def findMaximumExcluding(vec,n):
    v = copy.copy(vec);     # Copy the input vector
    v.pop(n)                # Remove the n-th element
    return max(v)           # return the minimum value of the remaining vector

## Shows a progressbar
def progress(val,end_val, bar_length=50):
    percent = float(val) / end_val
    hashes = '#' * int(round(percent * bar_length))
    spaces = ' ' * (bar_length - len(hashes))
    sys.stdout.write("\rProgress: [{0}] {1}%".format(hashes + spaces, int(round(percent * 100))))
    sys.stdout.flush()

## Least common multiple of two numbers
def lcm(a,b):
    """Return lowest common multiple."""
    return abs(a * b) / fractions.gcd(a,b) if a and b else 0

## Least common multiple of many numbers
def lcmm(*args):
    """Return lcm of args."""   
    return reduce(lcm, args)

## Read the input data from a csv file
def readInputData(filename):
    time = [];
    requests = [];
    with open(filename,'rb') as csvfile:
        reader = csv.reader(csvfile)
        try:
            for row in reader:
                time.append(int(row[0]))
                requests.append([float(i) for i in row[1:]])
            num_streams = len(requests[0])
            return time, requests, num_streams
        except csv.Error as e:
            sys.exit('file %s, line %d: %s' % (filename, reader.line_num, e))


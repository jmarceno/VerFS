from numba import jit
from numba.typed import List


from bisect import bisect_left

def take_closest(myList, myNumber):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    pos = bisect_left(myList, myNumber)
    if pos == 0:
        return myList[0], pos
    if pos == len(myList):
        return myList[-1], pos-1
    # before = myList[pos - 1]
    return myList[pos], pos
    # after = myList[pos]
    # if after - myNumber < myNumber - before:
    #    return after, pos
    # else:
    #    return before, pos-1


@jit(nopython=True) # Set "nopython" mode for best performance, equivalent to @njit
def offsets(data):
    
    offsets = List()
    [offsets.append(x+offsets[len(offsets)-1]) for x in data]
    offsets.pop(0)

    return offsets
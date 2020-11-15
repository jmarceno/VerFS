from numba import jit
from numba.typed import List


from bisect import bisect_left, bisect_right

def take_closest(myList, myNumber, left=True):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    if left:
        pos = bisect_left(myList, myNumber)
    else:
        pos = bisect_right(myList, myNumber)

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

    offs = List()
    offs.append(0)
    [offs.append(x[1]+offs[len(offs)-1]) for x in data]
    offs.pop(0)
    
    # offsets = List()
    # [offsets.append(x+offsets[len(offsets)-1]) for x in data]
    # offsets.pop(0)

    return offs
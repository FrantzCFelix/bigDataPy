#!/usr/bin/env python
"""reducer.py"""

# python mapper.py < input.txt | sort | python reducer.py
# hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /user/inputs/inaugs.tar.gz -output /user/j_singh/inaugs

import collections
from operator import itemgetter
import sys
import numpy as np
import pprint
pp = pprint.PrettyPrinter(indent=4, width=80)

# https://docs.google.com/document/d/17Fmco0wZGRnaPlRFm5QOLw-9J4PAf3saGGk8_fMoe7s/edit


def main(argv):
    current_word = None
    current_fnam = None
    current_count = 0
    word = None
    wcss = {}

    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line_ = line.strip()

        # parse the input we got from mapper.py
        _, word, fnam, count = line_.split('\t', 3)

        # convert count (currently a string) to int
        try:
            count = int(count)
            if fnam in wcss:
                wcss[fnam].update({word: count})
            else:
                wcss[fnam] = collections.Counter()
                wcss[fnam].update({word: count})

        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            pass
    # print(wcss)
    tfidf = calculateTFIDF(wcss)  # Implement this function

    # for foo in wcss:
    keys = list(wcss.items())
    # print(keys)
    # print("8=====================D")
    # print((list(keys[0])))
    # print((keys[0][1]['chuck']))
    # bCount = keys[0][1]
    # print(list(bCount))

    # print((list(keys[0][1])[0]))

    # wordset = list()

    for i in range(len(keys)):
        termSet = keys[i][1]
        print(termSet)
        print("8===============vxcvx======D")
        for j in range(len(list(termSet))):
            # print(list(termSet)[j])
            wordset.append(list(termSet)[j])

    print(wordset)


def calculateTF(wordset, bow):
    termfreq_diz = dict.fromkeys(wordset, 0)
    counter1 = dict(collections.Counter(bow))
    for w in bow:
        termfreq_diz[w] = counter1[w]/len(bow)
    return termfreq_diz


def calculateTFIDF(wcss):
    expected = '''
    fnam1
        word1 tfidf1
        word2 tfidf2
        word3 tfidf3
          ::
        wordn tfidfn
    '''

    for fnam in sorted(wcss):
        wcs = wcss[fnam]
        print('\n\n', fnam)
        sorted_wcs = dict(
            sorted(wcs.items(), key=lambda item: item[1], reverse=True))
        for w in sorted_wcs:
            print(w, sorted_wcs[w])
    return None


if __name__ == "__main__":
    main(sys.argv)

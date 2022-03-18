#!/usr/bin/env python
"""reducer.py"""

# python mapper.py < input.txt | sort | python reducer.py
# hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /user/inputs/inaugs.tar.gz -output /user/j_singh/inaugs

import collections
from operator import itemgetter
from re import A, sub
import sys
import numpy as np
import pandas as pd
import pprint
import collections

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

    # tfidf = calculateTFIDF(wcss)  # Implement this function

    a = wcss['a']
    b = wcss['b']
    c = wcss['c']
    finalWordSet = list()
    aSum = 0
    bSum = 0
    cSum = 0

    for key in a:
        finalWordSet.append(key)
        aSum = aSum + a[key]
    for key in b:
        finalWordSet.append(key)
        bSum = bSum + b[key]
    for key in c:
        finalWordSet.append(key)
        cSum = cSum + c[key]

    print('-------')

    tf1 = calculateTF(finalWordSet, aSum, a)
    tf2 = calculateTF(finalWordSet, bSum, b)
    tf3 = calculateTF(finalWordSet, cSum, c)
    idf_diz = calculate_IDF(finalWordSet, [a, b, c])

    # df_tf = pd.DataFrame([tf1, tf2, tf3])
    # df_idf = pd.DataFrame([idf_diz])
    # print(df_idf)
    # print(df_tf)


def calculate_IDF(wordset, bow):
    d_bow = {'bow_{}'.format(i): list(set(b)) for i, b in enumerate(bow)}
    # print('*******', d_bow)
    # {'bow_0': ['seashore', 'shells', 'seashells', 'surely', 'sells'], 'bow_1': ['woodchuck', 'wood', 'chuck'], 'bow_2': ['peck', 'peter', 'picked', 'peppers', 'pick', 'piper', 'pickled']}
    # d_bow is a object with bow0, bow1, bow2 as keys and each set as an array for values
    N = len(d_bow.keys())
    # N = 3

    l_bow = []
    for b in d_bow.values():
        l_bow += b
    # print(l_bow, '+++++++++++++++++++++')
    # ['shells', 'seashells', 'sells', 'seashore', 'surely', 'woodchuck', 'wood', 'chuck', 'pick', 'peter', 'peppers', 'peck', 'picked', 'pickled', 'piper']
    counter = dict(collections.Counter(l_bow))
    # print('COUNTER', counter)
    # COUNTER {'shells': 1, 'seashells': 1, 'sells': 1, 'seashore': 1, 'surely': 1, 'woodchuck': 1, 'wood': 1, 'chuck': 1, 'pick': 1, 'peter': 1, 'peppers': 1, 'peck': 1, 'picked': 1, 'pickled': 1, 'piper': 1}
    idf_diz = dict.fromkeys(wordset, 0)
    # print(idf_diz)
    # {'seashells': 0, 'seashore': 0, 'sells': 0, 'shells': 0, 'surely': 0, 'chuck': 0, 'wood': 0, 'woodchuck': 0, 'peck': 0, 'peppers': 0, 'peter': 0, 'pick': 0, 'picked': 0, 'pickled': 0, 'piper': 0}
    for w in wordset:
        idf_diz[w] = np.log((1+N)/(1+counter[w]))+1
    return idf_diz


def calculateTF(wordset, sum, subset):
    termfreq_diz = dict.fromkeys(wordset, 0)
    for w in subset:
        termfreq_diz[w] = subset[w]/sum
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

    # print(a)
    # print(collections.Counter(b).values())
    # print(b)
    # print(collections.Counter(c).values())
    # print(c)
    # print("8=====================D")
    # print((list(keys[0])))
    # print((keys[0][1]['chuck']))
    # bCount = keys[0][1]
    # print(list(bCount))

    # print((list(keys[0][1])[0]))

    # wordset = list()

    # for i in range(len(keys)):
    #     termSet = keys[i][1]
    #     print(termSet)
    #     print("8===============vxcvx======D")
    #     for j in range(len(list(termSet))):
    #         # print(list(termSet)[j])
    #         wordset.append(list(termSet)[j])

    # print(wordset)

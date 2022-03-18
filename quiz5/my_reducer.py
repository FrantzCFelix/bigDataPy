#!/usr/bin/env python
"""reducer.py"""

# python mapper.py < input.txt | sort | python reducer.py
# hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /user/inputs/inaugs.tar.gz -output /user/j_singh/inaugs
# https://docs.google.com/document/d/17Fmco0wZGRnaPlRFm5QOLw-9J4PAf3saGGk8_fMoe7s/edit


import collections
from operator import itemgetter
from re import A, sub
import sys
import numpy as np
import pprint
import collections

pp = pprint.PrettyPrinter(indent=4, width=80)


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

    tf_idf_1 = calculate_TF_IDF(finalWordSet,tf1,idf_diz)
    # print(tf_idf_1)
    tf_idf_2 = calculate_TF_IDF(finalWordSet,tf2,idf_diz)
    # print(tf_idf_2)
    tf_idf_3 = calculate_TF_IDF(finalWordSet,tf3,idf_diz)
    # print(tf_idf_3)
    print([tf_idf_1,tf_idf_2,tf_idf_3])
    # df_tfidf = pd.DataFrame([tf_idf_1,tf_idf_2,tf_idf_3])
    # print(df_tfidf.head())


def calculate_IDF(wordset, bow):
    d_bow = {'bow_{}'.format(i): list(set(b)) for i, b in enumerate(bow)}
    N = len(d_bow.keys())
    l_bow = []
    for b in d_bow.values():
        l_bow += b
        counter = dict(collections.Counter(l_bow))
    idf_diz = dict.fromkeys(wordset, 0)
    for w in wordset:
        idf_diz[w] = np.log((1+N)/(1+counter[w]))+1
    return idf_diz

def calculateTF(wordset, sum, subset):
    termfreq_diz = dict.fromkeys(wordset, 0)
    for w in subset:
        termfreq_diz[w] = subset[w]/sum
    return termfreq_diz

def calculate_TF_IDF(wordset,tf_diz,idf_diz):
    tf_idf_diz = dict.fromkeys(wordset,0)
    for w in wordset:
        tf_idf_diz[w]=tf_diz[w]*idf_diz[w]
    tdidf_values = list(tf_idf_diz.values())
    l2_norm = np.linalg.norm(tdidf_values)   
    print()
    tf_idf_norm = {w:tf_idf_diz[w]/l2_norm for w in wordset}
    return tf_idf_norm

if __name__ == "__main__":
    main(sys.argv)


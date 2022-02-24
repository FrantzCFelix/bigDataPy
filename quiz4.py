from scipy.sparse import coo_matrix
import numpy as np
import pandas as pd

print(np.version.version)
chicagoTaxiRidesDf = pd.read_csv('./chicago-taxi-rides.csv').dropna()

chicagoTaxiRidesDfCSV = chicagoTaxiRidesDf.to_csv('./test.csv', index=False)

# print(chicagoTaxiRidesDf)

l, c, v = np.genfromtxt('./test.csv',
                        skip_header=1, delimiter=',', usecols=range(3)).T

r = l.astype(int)
# for i in range(len(r)):
#     print(r)

# l, c, v = np.loadtxt('./chicago-taxi-rides.csv',
#                      delimiter=',', skiprows=1).T

# print(l, c, v)
m = coo_matrix((v, (r-1, c-1)), shape=(r.max(), c.max()))

# for i in c:
# print(i)
# https://note.nkmk.me/en/python-numpy-nan-remove/
# rowNum = int(r[~np.isnan(r)].max())
# colNum = int(c[~np.isnan(c)].max())
# print(int(rowNum))
# print(int(colNum))
# print(r.astype(int)-1)

# m = coo_matrix((v, (r, c)))
# numpy_array = np.loadtxt('./chicago-taxi-rides.csv',
#  delimiter=",", skiprows=1)
# print(numpy_array)
# print(r[~np.isnan(r)].max())
# print(r)
# print(m)
# shape=(l.max(), c.max()
# print(coo_matrix((3, 4), dtype=np.int8).toarray())
# arr = np.array([[0, 0, 0, 0],
#                 [0, 0, 0, 0],
#                 [0, 0, 0, 0]], dtype='int8')

# print(arr)

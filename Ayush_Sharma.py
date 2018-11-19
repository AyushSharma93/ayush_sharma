import pandas as pd
import csv
import traceback
import random

csvfile = open(r'F:\Emplay Analytics\submission.csv', 'w+', newline='')
filewriter = csv.writer(csvfile, delimiter=',',
                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
filewriter.writerow(['fullVisitorId', 'PredictedLogRevenue'])

check = set()


def process(chunk):
    for index, row in chunk.iterrows():
        try:
            id = str(row['fullVisitorId'])
            if id not in check:
                filewriter.writerow([id, 0])
                check.add(id)
        except Exception as e:
            print(row['fullVisitorId'])
            traceback.print_exc()
            return


chunksize = 10000
for chunk in pd.read_csv(r'F:\Emplay Analytics\all\test.csv', chunksize=chunksize, dtype={'fullVisitorId': object}):
    process(chunk)

csvfile.close()

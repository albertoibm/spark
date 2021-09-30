import pyarrow.parquet as pq
import pandas as pd

import argparse

parser = argparse.ArgumentParser(
    description="Indexes id from a parquet file to its filename and rowgroup"
)
parser.add_argument("-1", "--files1", help="Parquet files to index", type=str, nargs='*')
parser.add_argument("-2", "--files2", help="Parquet files to index against", type=str, nargs='*')
parser.add_argument("-c", "--column", help="Name of column to look for. If not given, will use first column of files1", type=str, default = None)


args = parser.parse_args()

if args.files1 == None or args.files2 == None:
    parser.print_help()
    exit()

colid = args.column

index = {}

for f1 in args.files1:
    pf1 = pq.ParquetFile(f1)
    col_id = colid if colid != None else pf1.schema.column(0).name
    for crg in range(pf1.num_row_groups):
        rg1 = pf1.read_row_group(crg, columns = [col_id])
        for f2 in args.files2:
            pf2 = pq.ParquetFile(f2)
            for org in range(pf2.num_row_groups):
                rg2 = pf2.read_row_group(org, columns = [col_id])
                cids = rg1.to_pandas()
                ocids = rg2.to_pandas()
                intersect = list(pd.merge(cids,ocids,how="inner")[col_id].values)
                if len(intersect) > 0:
                    index[f"{f1},{crg};{f2},{org}"] = intersect
import json
with open("index.json","w") as fopen:
    json.dump(index,fopen)

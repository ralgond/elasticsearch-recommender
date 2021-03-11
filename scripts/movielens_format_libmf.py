import sys
import os

indir="D:/ml-data/movielens/ml-1m"
outdir="D:/ml-data/movielens/ml-1m/libmf"

outfile1 = open(outdir+"/ratings.libmf", "w+")
for line in open(indir+"/ratings.dat"):
    uid, mid, score, _ = line.strip().split("::")
    outfile1.write(" ".join([uid, mid, score])+"\n")

outfile1.close()    



indir="D:/ml-data/movielens/ml-1m/libmf"
outdir="D:/ml-data/movielens/ml-1m/libmf"

import random

train_file = open(indir+"/ratings_train.libmf", "w+")
test_file = open(indir+"/ratings_test.libmf", "w+")

for line in open(indir+"/ratings.libmf"):
    if random.random() < 0.8:
        train_file.write(line)
    else:
        test_file.write(line)

train_file.close()
test_file.close()
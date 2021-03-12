

indir="D:/ml-data/movielens/ml-1m"
outdir="D:/ml-data/movielens/ml-1m/libmf"

userratehistory = {}

for line in open(indir+"/ratings.dat"):
    uid, mid, _, _ = line.strip().split("::")
    histiroyList = userratehistory.get(uid, [])
    histiroyList.append(mid)
    userratehistory[uid] = histiroyList


outf = open(outdir+"/user_rate_history.dat", "w+")
for uid, histiroyList in userratehistory.items():
    outf.write(uid+"\t"+','.join(histiroyList)+"\n")
outf.close
    

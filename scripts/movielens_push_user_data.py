from elasticsearch import Elasticsearch

indir="D:/ml-data/movielens/ml-1m"
outdir="D:/ml-data/movielens/ml-1m/libmf"

model_file = open(outdir+"/ratings_train.libmf.model")
model_file.readline()
model_file.readline()
model_file.readline()
model_file.readline()
model_file.readline()

user_history_tbl = {}
for line in open(outdir+"/user_rate_history.dat"):
    uid, history = line.strip().split()
    user_history_tbl[int(uid)] = [int(f) for f in history.split(",")]

class User:
    def __init__(self, id, model_factor, rate_history):
        self.id = id
        self.model_factor = model_factor
        self.rate_history = rate_history

user_list = []

for line in model_file:
    if not line.startswith("p"):
        break
    else:
        arr = line.strip().split()
        if arr[1] == 'F':
            continue
        model_factor = []
        for f in arr[2:]:
            model_factor.append(float(f))
        uid = int(arr[0][1:])
        u = User(uid, model_factor, user_history_tbl.get(uid, []))
        user_list.append(u)

print(len(user_list))
print (user_list[0].id), print(user_list[0].model_factor), print(user_list[0].rate_history)

es = Elasticsearch()

for u in user_list:
    data = {
        "userId": u.id,
        "model_factor": u.model_factor,
        "rate_history": u.rate_history
    }
    es.index(index="users", body=data, id=u.id)

es.close()
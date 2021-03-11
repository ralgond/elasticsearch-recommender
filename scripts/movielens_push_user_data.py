from elasticsearch import Elasticsearch

indir="D:/ml-data/movielens/ml-1m"
outdir="D:/ml-data/movielens/ml-1m/libmf"

model_file = open(outdir+"/ratings_train.libmf.model")
model_file.readline()
model_file.readline()
model_file.readline()
model_file.readline()
model_file.readline()

class User:
    def __init__(self, id, model_factor):
        self.id = id
        self.model_factor = model_factor

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
        u = User(int(arr[0][1:]), model_factor)
        user_list.append(u)

print(len(user_list))
print (user_list[0].id), print(user_list[0].model_factor)

es = Elasticsearch()

for u in user_list:
    data = {
        "userId": u.id,
        "model_factor": u.model_factor
    }
    es.index(index="users", body=data, id=u.id)

es.close()
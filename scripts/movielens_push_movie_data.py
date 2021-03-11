from elasticsearch import Elasticsearch

indir="D:/ml-data/movielens/ml-1m"
outdir="D:/ml-data/movielens/ml-1m/libmf"

movieId2name = {}
movieId2name[0] = "NaN"
for line in open(indir+"/movies.dat", 'r', encoding='iso-8859-15'):
    id, name, _ = line.strip().split("::")
    movieId2name[int(id)] = name

model_file = open(outdir+"/ratings_train.libmf.model")
model_file.readline()
model_file.readline()
model_file.readline()
model_file.readline()
model_file.readline()

class Movie:
    def __init__(self, id, name, model_factor):
        self.id = id
        self.name = name
        self.model_factor = model_factor

movie_list = []

for line in model_file:
    if line.startswith("q"):
        arr = line.strip().split()
        if arr[1] == 'F':
            continue
        id = int(arr[0][1:])
        #print(id)
        name = movieId2name.get(id, "NaN")
        model_factor = [float(f) for f in arr[2:]]
        m = Movie(id, name, model_factor)
        movie_list.append(m)

print(len(movie_list))

es = Elasticsearch()

for m in movie_list:
    data = {
        "movieId": m.id,
        "movieName": m.name,
        "model_factor": m.model_factor
    }
    es.index(index="movies", body=data, id=m.id)

es.close()
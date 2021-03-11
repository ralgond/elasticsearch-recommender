from elasticsearch import Elasticsearch
import sys

indir="D:/ml-data/movielens/ml-1m"

movieId2name = {}
movieId2name[0] = "NaN"
for line in open(indir+"/movies.dat", 'r', encoding='iso-8859-15'):
    id, name, _ = line.strip().split("::")
    movieId2name[int(id)] = name

userId = int(sys.argv[1])

es = Elasticsearch()

movie = es.get(index="users", id=userId)

model_factor = movie['_source']["model_factor"]


def vector_query(query_vec, vector_field, q="*", cosine=False):
    if cosine:
        score_fn = "doc['{v}'].size() == 0 ? 0 : cosineSimilarity(params.vector, '{v}') + 1.0"
    else:
        score_fn = "doc['{v}'].size() == 0 ? 0 : sigmoid(1, Math.E, -dotProduct(params.vector, '{v}'))"
       
    score_fn = score_fn.format(v=vector_field, fn=score_fn)
    
    return {
    "query": {
        "script_score": {
            "query" : { 
                "query_string": {
                    "query": q
                }
            },
            "script": {
                "source": score_fn,
                "params": {
                    "vector": query_vec
                }
            }
        }
    }
    }

q = vector_query(model_factor, "model_factor", q="*", cosine=False)
print(q)
print("")

results = es.search(index="movies", body=q)
movie_list = results['hits']['hits']
for m in movie_list:
    print(m['_source']['movieName'])

es.close()
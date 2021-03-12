from elasticsearch import Elasticsearch
import sys

indir="D:/ml-data/movielens/ml-1m"

movieId2name = {}
movieId2name[0] = "NaN"
for line in open(indir+"/movies.dat", 'r', encoding='iso-8859-15'):
    id, name, _ = line.strip().split("::")
    movieId2name[int(id)] = name

movieId = int(sys.argv[1])

es = Elasticsearch()

movie = es.get(index="movies", id=movieId)

model_factor = movie['_source']["model_factor"]


def vector_query(movieId, query_vec, vector_field, q="*"):

    score_fn = "doc['{v}'].size() == 0 ? 0 : cosineSimilarity(params.vector, '{v}') + 1.0"
       
    score_fn = score_fn.format(v=vector_field, fn=score_fn)
    
    return {
    "query": {
        "script_score": {
            "query" : { 
                "bool": {
                    "must_not": {
                        "term": {
                            "_id": movieId
                        }
                    }
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

q = vector_query(movieId, model_factor, "model_factor", q="*")
print(q)
print("")

results = es.search(index="movies", body=q)
movie_list = results['hits']['hits']
for m in movie_list:
    print(m['_source']['movieName'])

es.close()

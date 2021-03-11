from elasticsearch import Elasticsearch

VECTOR_DIM = 20
create_users = {
    # this mapping definition sets up the metadata fields for the users
    "mappings": {
        "properties": {
            "userId": {
                "type": "integer"
            },
            # the following fields define our model factor vectors and metadata
            "model_factor": {
                "type": "dense_vector",
                "dims": VECTOR_DIM
            },
        }
    }
}

create_movies = {
    # this mapping definition sets up the metadata fields for the movies
    "mappings": {
        "properties": {
            "movieId": {
                "type": "integer"
            },
            "movieName": {
                "type": "keyword"
            },
            # the following fields define our model factor vectors and metadata
            "model_factor": {
                "type": "dense_vector",
                "dims": VECTOR_DIM
            }
        }
    }
}

es = Elasticsearch()
es.indices.create(index="users", body=create_users)
es.indices.create(index="movies", body=create_movies)

es.close()
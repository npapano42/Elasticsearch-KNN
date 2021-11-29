import random

from elasticsearch import Elasticsearch
import pandas as pd
import requests

# Defaults to localhost:9200
es = Elasticsearch()


def create_demo_index():
    es.indices.delete(index='demo', ignore=[400, 404])
    mapping = {
        "properties": {
            "body": {
                "type": "text",
                # "similarity": "scripted_tfidf"
            },
            "doc_id": {
                "type": "long",
            }
        }
    }
    # settings = { "number_of_shards": 1, "similarity": { "scripted_tfidf": { "type": "scripted", "script": {
    # "source": "double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) +
    # 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;" } } } }
    es.indices.create(index="demo",
                      mappings=mapping,
                      # settings=settings
                      )
    print(es.indices.get_mapping(index="demo"))


def demo_read_data():
    data = {
        "query": {
            "query_string": {
                "query": "(Hello)",
                "default_field": "body"
            }
        }
    }
    res = requests.request(method="get", url="http://localhost:9200/_search", json=data)
    print(res.json())


def create_kickstarter_index():
    es.indices.delete(index='kickstarter', ignore=[400, 404])
    mapping = {
        "properties": {
            "desc": {
                "type": "text",
            },
            "state": {
                "type": "text",
            },
            "doc_id": {
                "type": "long",
            }
        }
    }
    es.indices.create(index="kickstarter", mappings=mapping)
    print(es.indices.get_mapping(index="kickstarter"))


def load_kickstarter_data():
    dataframe = pd.read_csv("./data/kickstarterData.csv", skiprows=1)

    for row in dataframe.iterrows():
        rand_int = random.randint(0, 2000000000)
        doc = {"doc_id": rand_int, "desc": row[1][1], "state": row[1][2]}
        res = es.index(index="kickstarter", doc_type="_doc", id=rand_int, document=doc)
        print(res)


def convert_to_query(tokens):
    return "(" + ") or (".join(tokens) + ")"


def elasticsearch_query(tokens, k=10):
    data = {
        # number of neighbors to consider in k nearest neighbors
        "size": k,
        "query": {
            "query_string": {
                "query": convert_to_query(tokens),
                "default_field": "desc"
            }
        }
    }

    res = requests.request(method="get", url="http://localhost:9200/kickstarter/_search?pretty&explain", json=data)
    hits = res.json()["hits"]["hits"]

    # tally up
    matches = {"successful": 0, "failed": 0}
    for item in hits:
        matches[item["_source"]["state"]] += 1

    # quick metric
    print({a: matches[a] / k for a in matches})

    # classification
    print("Resulting class:", max(matches, key=matches.get))


if __name__ == '__main__':
    # print("k = 10")
    # elasticsearch_query(["Stuff"])
    # print("k = 25")
    # elasticsearch_query(["Stuff"], k=25)
    # print("k = 40")
    # elasticsearch_query(["Stuff"], k=40)

    kickstarter_desc = "The Sailors Compass, in brass, is a navigational tool created for the boldest of explorers" \
                       "A technical aid whose visual balance remains true to its aesthetic heritage "

    for i in range(20, 60, 5):
        elasticsearch_query(kickstarter_desc.split(" "), k=i)

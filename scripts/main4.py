from datetime import datetime
from unittest import result
from elasticsearch import Elasticsearch, NotFoundError, RequestError
from elasticsearch import RequestsHttpConnection
from elasticsearch import helpers
from scripts.main2 import _seperator, check_connection, get_all_documents, get_all_indices, get_mapping, write_document, write_index
import json

# disable các warning khi run scripts
import warnings
warnings.filterwarnings('ignore')

# Kết nối python tới ES
es = Elasticsearch(
    ["localhost"], 
    port=9200, 
    connection_class=RequestsHttpConnection, 
    http_auth=("elastic", "elastic"), 
    use_ssl=True, 
    verify_certs=False
)

def test_extension():
    """_summary_
    """
    àe = {'a': 5,}

if __name__ == "__main__":

    # i =10
    # f = open('data.json')
    # for item in f:
    #     if i > 0:
    #         data = json.loads(item)
            
            # print(data)
            # i-=1
    # f.close()
    # write_index('post')

    # -------------------------------------------------------------------------

    # get_all_indices()
    # get_all_documents('my_users')

    # A match query looks for the existence of a token in a field, whereas a match_phrase query looks for the existence of a sequence of tokens (a phrase) in the field.
    query = {
        "query": {
            "match_phrase": {
                "district": "Go Vap"
            }
        }
    }

    query2 = {
        "query": {
            "match": {
                "district.keyword": "Go Vap"
            }
        }
    }
    # res = es.search(index="my_users", body=query2)
    # for item in res['hits']['hits']:
    #     print(item['_id'],item['_source'])
    
    
    get_all_documents('hd_logger_db')
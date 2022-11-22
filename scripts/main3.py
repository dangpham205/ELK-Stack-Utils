from datetime import datetime
from elasticsearch import Elasticsearch, NotFoundError, RequestError
from elasticsearch import RequestsHttpConnection
from elasticsearch import helpers
from scripts.main2 import _seperator, check_connection, get_all_documents, get_mapping, write_document, write_index
from fake_data.sample_data import dummy
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

if __name__ == "__main__":
    # _seperator()
    # check_connection()

    body = {
        "mappings": {
            "properties": {
                "city": {
                    "type": "keyword"
                },
                "district": {
                    "type": "keyword"
                },
                "user_id": {
                    "type": "keyword"
                },
            }
        }
    }
    # print(write_index('mai_users', body=body))

    # res = es.reindex({
    #     "source": {
    #         "index": "my_users"
    #     },
    #     "dest": {
    #         "index": "mai_users"
    #     }
    # })
    # print(res)
    # ----------------------------------------------------------------

    # res = es.delete(index='mai_users', id = 'u1u2u3', refresh='true')
    # print(res)

    # ----------------------------------------------------------------

    # get_mapping('my_users')

    query = {
        "query": {
            "match_all": {}
            # "match": { 
            #     "city" : 'TPHCM DA'
            # },
        },
        "sort": [
            # {"city": "asc"}               # TH field type là keyword
            # {"user_id.keyword": "desc"}   # TH field type là keyword + text ==> .keyword để có thể sort 
            {"time": "desc"}                # TH field type là date thì sẽ so sánh tới second (21:00:00.32 và 21:00:00.54 sẽ bằng nhau)
        ]
    }
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "time": {
                                "gte": "2022-10-17T17:00:00"
                            }
                        }
                    }
                ]
            }
        }
    }
    res = es.search(index="mai_users", body=query)
    for item in res['hits']['hits']:
        print(item['_source'])

    # get_all_documents('mai_users')
    
    # res = es.search(index='mai_users', q='TPHCM')
    # res = es.search(index='mai_users', q='2022-10-17')
    # res = es.search(index='mai_users', q='[2022-10-17 TO 2022-10-18]')
    # res = es.search(index='mai_users', q='[2022-10-17 TO now]')
    # for item in res['hits']['hits']:
    #     print(item['_source'])

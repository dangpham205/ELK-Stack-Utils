from datetime import datetime
from elasticsearch import Elasticsearch, NotFoundError, RequestError
from elasticsearch import RequestsHttpConnection
from elasticsearch import helpers
from scripts.main2 import _seperator, check_connection, delete_index, get_all_documents, get_all_indices, get_mapping, write_document, write_index
import json
from datetime import date

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
    get_all_indices()
    # delete_index('my_posts')
    # write_index('my_posts')

    post = {
        "id": "p4",
        "title": "Post 4",
        "date": date(2022,10,2),
        "prior": False,
        "status": 2,
        "primary": "Category 3"
    }

    # get_mapping('my_posts')
    # write_document(index="my_posts", doc_body=post, doc_id = "p4")

    day_to_search = "2022-09-10"
    # title_to_search = '*2'
    title_to_search = False
    # status_to_search  = False
    status_to_search  = 1
    status_start = status_to_search if status_to_search else 0
    stautus_end = status_to_search + 1 if (status_to_search is not False) else 3
    print(f'start {status_start} | end {stautus_end}')

    query = {
        "query": {
            "bool": {
                "must": [
                    # {
                    #     "range": {
                    #         "date": {
                    #             "gte": "2022-09",    #interval 1 ngày
                    #             "lt": "2022-09-11",

                    #             "gte": "09",
                    #             "lte": "10",
                    #             "format": "year_month"
                    #         } 
                    #     }
                    # },
                    {
                        "wildcard": {
                            "title.keyword": {
                                "value": title_to_search
                            }
                        }
                    } if title_to_search else None,
                    None,                                       #(*)
                    {
                        "range": {
                            "status": {
                                "gte": status_start,
                                "lt": stautus_end
                                # "value": status_to_search
                            }
                        }
                    }
                ]
            }
        }
    }

    # res = es.search(index="my_posts", body=query)
    # for item in res['hits']['hits']:
    #     print(item['_source'])
    
    # res = es.search(index='my_posts', q='date:2022-09-10')
    # for item in res['hits']['hits']:
    #     print(item['_source'])

    # get_all_documents('my_posts')
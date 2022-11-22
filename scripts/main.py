from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
import io

# Kết nối python tới ES
es = Elasticsearch(
    ["localhost"], 
    port=9200, 
    connection_class=RequestsHttpConnection, 
    http_auth=("elastic", "elastic"), 
    use_ssl=True, 
    verify_certs=False
)
# vì ES default run port 9200, nên nếu để port khác sẽ ra False

# KIỂM TRA KẾT NỐI
# print("Connection status: ",es)    # True
# print("Connection status: ",es.info())    # True
# print("Connection status: ",es.ping())    # True


# all_indices = es.cat.indices(format="json")
# for i in all_indices:
#     print(i)

# create index
# chỉ có thể create 1 lần, vì tên table cần unique, kh thể có 2 table trùng tên
# res =es.indices.create(index='tutorial2')
# print(res)

# display all indices
indices = es.indices.get_alias('*') 
# print('All indices: ', indices)
# for i in indices:
#     print(i)

# create nhiều index at the same time
# index_base= "september"
# for i in range(1,4):
#     response = es.indices.create(index=index_base+"_day_"+str(i))
#     print(response)

# read indices.txt file
# with io.open("indices.txt", "r", encoding="utf-8") as file:
#     data = file.read()
#     file.close()
# data = data.split("\n")
# print(data)

# check if index exists
# print("Exists: ",es.indices.exists(index="tutorial1"))

# delete index
# es.indices.delete(index="tutorial2")
# print("Exists: ",es.indices.exists(index="tutorial2"))

# write document
doc1 = {
    "age": 30,
    "country": "United States" 
} 
doc2 = {
    "age": 23,
    "country": "Viet Nam" 
} 
# response = es.index(index="tutorial1", id = 1, body=doc1)
# response = es.index(index="tutorial1", id = 2, body=doc2)
# response = es.update(index="tutorial1", id = 2, body=doc1)
# response = es.delete(index="tutorial1", id = 2)
# print(response['result'])

# get document with id = 2 from index "messi"
# res = es.get(index="tutorial1", doc_type="places", id=2)
# res = es.get(index="tutorial1", id=1)
# res = es.get(index="tutorial1", id=2)
# print(res)
# print(res['_source'])

# trả về body của tất cả document thuộc 'tutorial1' 
body = {
    "query": {
        "match_all": {}
    }
}
# trả về body của tất cả document thuộc 'tutorial1' có field 'country' chứa từ 'viet' OR 'states'  
# mặc định là OR
body = {
    "query": {
        "match": {
            "country": {
                "query": "viet states"
            }
        }
    }
}
# trả về body của tất cả document thuộc 'tutorial1' có field 'country' chứa từ 'viet' AND 'states'  
body = {
    "query": {
        "match": {
            "country": {
                "query": "viet states",
                "operator": "AND"
            }
        }
    }
}
# vấn đề khi dùng AND hoặc OR, precision hoặc recall sẽ cao hẳn, gây ra mất cân bằng, result trả về sẽ quá nhiều hoặc quá ít
# ====> truyền minimum_should_match :  trong 4 từ bên dưới cần match ít nhất 3 từ
body = {
    "query": {
        "match": {
            "country": {
                "query": "viet states russia laos",
                "minimum_should_match": 3
            }
        }
    }
}
# search docs có cụm từ "united States" theo đúng thứ tự được ghi 
{
  "query": {
    "match_phrase": {
      "country": "united States "
    }
  }
}
# search xem từ field country xuất hiện bao nhiêu lần trong các document thuộc 'tutorial1'
body = (
{
    "size": 0,              # số hits muốn trả về, set  = 0 vì chỉ muốn coi kết quả của aggs
    "aggs": {
        "group_by_country": {       #tên của aggregation (tự đặt)
            "terms": {
                "size": 4000,       #số document trả về (nếu có thể)
                "field": "country.keyword"      # nếu muốn tổng hợp dựa trên kw thì .keyword. Còn tổng hợp value thì không cần
            }
        }
    }
}
)
# res = es.search(index="tutorial1", body={})
res = es.search(index="tutorial1", body=body)
print(res)
# print("Got %d hits: " % res['hits']['total']['value'])
# for hit in res['hits']['hits']:
    # print("%(age)s %(country)s" % hit["_source"])
#     print(hit['_source'])
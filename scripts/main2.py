from elasticsearch import Elasticsearch, NotFoundError, RequestError
from elasticsearch import RequestsHttpConnection
from elasticsearch import helpers
import time

from fake_data.sample_data import users, delivery_worker

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

def _seperator():
    print('-------------------------------')

def check_connection():
    print("Connection status: ",es.ping())    # True


def write_index(index_name, body = None):
    try:
        if check_index_existed(index_name):
            print('Index already exists, cant create')
            return False
        else:
            res = es.indices.create(index=index_name, body = body)
            return res['acknowledged']
    except:
        return False

def get_all_indices():
    print('All indices:')
    _seperator()
    indices = es.indices.get_alias('*') 
    for i in indices:
        print(i)
    _seperator()

def check_index_existed(index_name):
    exists = es.indices.exists(index = index_name)
    return exists


def delete_index(index_name):
    exists = check_index_existed(index_name)
    if exists:
        es.indices.delete(index = index_name)
        print(f'Index [ {index_name} ] deleted')
        return True
    else:
        print(f'Index [ {index_name} ] does not exist')
        return False

def refresh(index_name):
    es.indices.refresh( index = index_name )


def write_document(index, doc_body, doc_id = None):
    '''
        Index được truyền vô bắt buộc phải tồn tại từ trc\n
        Hàm này sẽ write đè nếu doc_id trong index đã tồn tại\n
        Nếu không truyền doc_id, 1 random ID sẽ đc tạo
    '''
    exists = check_index_existed(index)
    if exists:
        response = es.index(index= index, id = doc_id, body = doc_body, refresh = 'true')
        print('Write document: ', response['result'])
    else:
        print('Index does not exist, can not write document.')

def update_document(index, doc_id, doc_body):
    '''
        Index được truyền vô bắt buộc phải tồn tại từ trc\n
        Hàm này sẽ cho phép thêm fields / update giá trị các field trong document
    '''
    exists = check_index_existed(index)
    if exists:
        try:
            response = es.update(index = index, id = doc_id, body = doc_body, refresh= 'true')
            print('Update document: ', response['result'])
        except NotFoundError:
            print('Document does not exist, can not update document.')
    else:
        print('Index does not exist, can not update document.')

def get_all_documents(index, size = None):
    '''
        trả về body của tất cả document thuộc 'index'\n
        input:
            index: tên index
            size: số document muốn trả về 
    '''
    body = {
        "query": {
            "match_all": {}
        }
    }
    try:
        res = es.search(index = index, body = body, size = size)
        total_docs = res['hits']['total']['value']
        relation = res['hits']['total']['relation']
        list_docs = res['hits']['hits']

        _seperator()
        print(f'All documents in [ {index} ]:')
        print('Number: ', total_docs )
        print('Relation: ', relation )
        for hit in list_docs:
            print(f'id: {hit["_id"]} , {hit["_source"]}')
        _seperator()

    except:
        print('Index not found')
    


def delete_document(index, doc_id):
    try:
        response = es.delete(index = index, id = doc_id, refresh = 'true')
        print('Delete document: ',response['result'])
    except NotFoundError:
        print('Sth went wrong, make sure index and doc_id already exist')

def write_document_bulk():
    actions = []
    for i in users:
        actions.append({
            # default op_type sẽ là 'index'
            # "_op_type": "index", create, delete, update
            "_index": "mai_users",
            "_id": i['user_id'],
            "_source": i
        })
    helpers.bulk(es, actions)
    refresh(index_name='my_users')

def scan_document_bulk(index):
    query = {
        "query": {
            "match_all": {}
        }
    }
    start = time.time()
    result = helpers.scan(es, index=index, query= query)
    for item in result:
        print(item)
    end = time.time()
    print('Cost: ', end - start)

def write_template(template):
    write_document(index='templates', doc_id= template['template_id'], doc_body= template)
    refresh('templates')

def write_first_step(template_id, part_list = None, position_list = None):
    body = {
        "doc": {
            "parts": part_list,
            "positions": position_list
        }
    }
    update_document(index='templates', doc_id = template_id, doc_body = body)

def get_info_user(user_id):
    '''
        return list parts + list positions
    '''
    body = {
        "query": {
            "match_phrase": {
                "user_id": user_id
            }
        }
    }
    try:
        res = es.get(index="delivery_workder", id=user_id)
        parts = res['_source']['parts']
        positions = res['_source']['positions']
        print('Parts: ', parts)
        print('Positions', positions)
        return parts, positions

    except NotFoundError:
        print('Cant get user info ( index/id not exists) ')
        return False


def get_templates_for_user(user_id):
    try:
        parts, positions = get_info_user(user_id)
    except:
        print('Cant get user info ')
        return
    body = {
        "query": {
            "bool": {
                "should": [
                    {
                        "terms": { "positions" : positions},
                    },
                    {
                        "terms": { "parts" : parts},
                    },

                ]
            }
        },
    }
    res = es.search(index="templates", body=body)
    for hit in res['hits']['hits']:
        print(f'id: {hit["_id"]} , {hit["_source"]}')
    output = []
    for i in res['hits']['hits']:
        output.append(i['_source'])
    return output

def get_templates_by_folder_id(folder_id):
    body = {
        "query": {
            "match_phrase": {
                "folder_id": folder_id
            }
        }
    }
    res = es.search(index="templates", body=body)
    for hit in res['hits']['hits']:
        print(f'id: {hit["_id"]} , {hit["_source"]}')
    output = []
    for i in res['hits']['hits']:
        output.append(i['_source'])
    return output

def get_mapping(index):
    res = es.indices.get_mapping(index)
    # print(res)
    try:
        print(res[index]['mappings']['properties'].keys())
        print(res[index]['mappings'])
    except KeyError:
        print(f'Index [ {index} ] chưa được định nghĩa mapping ')
    return



if __name__ == '__main__':
    # check_connection()

    body = {
        "mappings": {
            "properties": {
                "name": {
                    "type": "text"
                },
                "produce_type": {
                    "type": "keyword"
                },
                "quantity": {
                    "type": "long"
                },
                "unit_price": {
                    "type": "float"
                },
                "vendor_details": {
                    "enabled": False
                }
            }
        }
    }
    # ghi index với mapping đc định nghĩa trc
    # print(write_index('products_sale3', body=body))
    # print(write_index('products_sale3'))

    body = {
        "runtime": {
            "total": {
                "type": "double",
                "script": {
                    "source": "emit( doc['price'].value * doc['quantity'].value )",
                    "lang": 'painless'
                }
            }
        }
    }
    # es.indices.put_mapping(index='products_sale', body= body)
    get_mapping('products_sale')

    # get_all_indices()

    # print(check_index_existed('products_sale'))
    # delete_index('employees')

    doc2 = {
        "product_name": "Nước", 
        "price": '12das',
        "quantity": '2dsa'
    }
    # write_document(index='products_sale', doc_id = 4, doc_body=doc2)
    # write_document(index='employees', doc_body=doc2)


    doc2 = {
        "doc": {
            "token": 'as'
            # "country": "USA" 
        }
    }
    # update_document(index='test_index', doc_id = 1, doc_body = doc2)
    # res = es.get(index='test_index', id =1)
    # print(res)


    # delete_document( 'test_index', 'wZcs1IMBH39TiFMISC31')

    get_all_documents('products_sale')

    body = {
        "size": 0,
        "aggs": {
            "total_sale": {
                "sum": {
                    "field": "total"
                }
            }
        }
    }
    # res = es.search(index="products_sale", body=body)
    # print(res)

    # ---------------------------------------------------------------------------------------

    # Lấy doc có id thuộc index (phải tự bắt lỗi index/id kh tồn tại)
    # res = es.get(index='test_index', id =1)
    # print(res['_source'])

    # scan_document_bulk('delivery_workder')
    # get_all_documents('delivery_workder')
    # es.transport.connection_pool.close()



    # ---------------------------------------------------------------------------------------

    # get_all_indices()

    # write data to delivery_workder table
    write_document_bulk()

    
    # Khi tạo ra template 
    temp = {
        'template_id': 't5',
        'template_name': 'template 5',
        'folder_id': 'f2',
        'folder_name': 'folder 2',
    }
    # write_template(temp)
    # get_all_documents('templates')

    # Khi tạo/modify step bắt đầu 
    # write_first_step('t5', position_list=[4])
    # get_all_documents('my_users')

    # LẤY TEMPLATES CHO NGƯỜI DÙNG
    # get_info_user('u1u2u3')
    # get_templates_for_user('u2')

    # get_templates_by_folder_id('f3')

    # get_all_documents('delivery_workder')
    # get_all_documents('templates',size= 3)
    # get_info_user('u1u2u3wqa')
    # es.transport.connection_pool.close()
    # check_connection()
from elasticsearch import Elasticsearch, NotFoundError, RequestsHttpConnection, helpers
import time

class MyElasticSearch():
    import warnings
    warnings.filterwarnings('ignore')

    es = None
    
    def __init__(self, domain = 'localhost', port = 9200, username = 'elastic', password = 'elastic'):
        self.es = Elasticsearch(
            ["localhost"], 
            port=9200, 
            connection_class=RequestsHttpConnection, 
            http_auth=("elastic", "elastic"), 
            use_ssl=True, 
            verify_certs=False
        )
        
    def _seperator(self, x):
        output = ''
        for i in range(40):
            output+=x
        print(output)
        
    def check_connection(self):
        print("Connection status: ",self.es.ping())    # True


    def write_index(self, index_name, body = None):
        try:
            if self.check_index_existed(index_name):
                print('Index already exists, cant create')
                return False
            else:
                res = self.es.indices.create(index=index_name, body = body)
                return res['acknowledged']
        except:
            return False

    def get_all_indices(self):
        self._seperator('X')
        print('All indices:')
        self._seperator('-')
        indices = self.es.indices.get_alias('*') 
        for i in indices:
            print(i)
        self._seperator('X')

    def check_index_existed(self, index_name):
        exists = self.es.indices.exists(index = index_name)
        return exists


    def delete_index(self, index_name):
        exists = self.check_index_existed(index_name)
        if exists:
            self.es.indices.delete(index = index_name)
            print(f'Index [ {index_name} ] deleted')
            return True
        else:
            print(f'Index [ {index_name} ] does not exist')
            return False

    def refresh(self, index_name):
        self.es.indices.refresh( index = index_name )


    def write_document(self, index, doc_body, doc_id = None):
        '''
            Index được truyền vô bắt buộc phải tồn tại từ trc\n
            Hàm này sẽ write đè nếu doc_id trong index đã tồn tại\n
            Nếu không truyền doc_id, 1 random ID sẽ đc tạo
        '''
        exists = self.check_index_existed(index)
        if exists:
            response = self.es.index(index= index, id = doc_id, body = doc_body, refresh = 'true')
            print('Write document: ', response['result'])
        else:
            print('Index does not exist, can not write document.')

    def update_document(self, index, doc_id, doc_body):
        '''
            Index được truyền vô bắt buộc phải tồn tại từ trc\n
            Hàm này sẽ cho phép thêm fields / update giá trị các field trong document
        '''
        exists = self.check_index_existed(index)
        if exists:
            try:
                response = self.es.update(index = index, id = doc_id, body = doc_body, refresh= 'true')
                print('Update document: ', response['result'])
            except NotFoundError:
                print('Document does not exist, can not update document.')
        else:
            print('Index does not exist, can not update document.')

    def get_all_documents(self, index, size = 5):
        '''
            trả về body của tất cả document thuộc 'index'\n
            input:
                index: tên index
                size: số document muốn trả về (default = 5)
        '''
        body = {
            "query": {
                "match_all": {}
            }
        }
        output = []
        try:
            res = self.es.search(index = index, body = body, size = size)
            total_docs = res['hits']['total']['value']
            relation = res['hits']['total']['relation']
            list_docs = res['hits']['hits']

            self._seperator('-')
            print(f'All documents in [ {index} ]:')
            print('Number: ', total_docs )
            print('Relation: ', relation )
            for hit in list_docs:
                output.append(hit)
                print(f'id: {hit["_id"]} , {hit["_source"]}')
                print('')
            self._seperator('-')
            return output

        except:
            print('Index not found')
        


    def delete_document(self, index, doc_id):
        try:
            response = self.es.delete(index = index, id = doc_id, refresh = 'true')
            print('Delete document: ',response['result'])
        except NotFoundError:
            print('Sth went wrong, make sure index and doc_id already exist')

    def write_document_bulk(self, index, list_data_json ):
        actions = []
        for i in list_data_json:
            actions.append({
                # default op_type sẽ là 'index'
                # "_op_type": "index", create, delete, update
                "_index": index,
                "_id": i['user_id'],
                "_source": i
            })
        helpers.bulk(self.es, actions)
        self.refresh(index_name='my_users')

    def scan_document_bulk(self, index):
        query = {
            "query": {
                "match_all": {}
            }
        }
        start = time.time()
        result = helpers.scan(self.es, index=index, query= query)
        for item in result:
            print(item)
        end = time.time()
        print('Cost: ', end - start)
from ES import MyElasticSearch

if __name__ == '__main__':
    es = MyElasticSearch()
    # es.check_connection()
    # es.get_all_documents('hd_logger_db')
    # es.write_index('sad')
    # es.get_all_indices()
    print(es.check_index_existed('hd_logger_db'))
    # es.delete_index('hd_logger_db')
    # es.get_all_documents('hd_logger_db')
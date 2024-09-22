from elasticsearch import Elasticsearch
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

es = Elasticsearch(hosts={"http://localhost:9200/"})

def index_data(index, doc_type, id, body):
    try:
        es.index(index=index, doc_type=doc_type, id= id, body=body)
        logger.info(f"Data indexed in ElasticSearch: {id}")

    except Exception as e:
        logger.error(f"Error indexing data: {str(e)}")

def search_data(index, body):
    try:
        result= es.search(index=index, body=body)
        logger.info(f"Search result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error Searching data: {str(e)}")
        return None
    
def delete_index(index):
    try:
        es.indices.delete(index=index, ignore=[400, 404])
        logger.info(f"Index deleted: {index}")
    except Exception as e:
        logger.error(f"Error deleting index: {str(e)}")

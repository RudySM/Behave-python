__author__ = 'rsantamaria'

import elasticsearch
from elasticsearch import Elasticsearch
import requests
import json

# es = elasticsearch.Elasticsearch(hosts='search-gregspike-okhncd7xkmnpgekdul3bfzwjdq.us-west-2.es.amazonaws.com:80')

es = Elasticsearch([{'host': 'search-gregspike-okhncd7xkmnpgekdul3bfzwjdq.us-west-2.es.amazonaws.com', 'port': 80}])
print es

ess = requests.get('https://search-spike-6k6tkmdnery6zrqm4bngmqrsgy.us-west-2.es.amazonaws.com')
print ess.status_code
print ess.text

content = '{"rudy":[{"test_components":[{"component1":"value1","component2":"value2","component3":"value3"}]}]}'

eess = Elasticsearch(hosts='https://search-spike-6k6tkmdnery6zrqm4bngmqrsgy.us-west-2.es.amazonaws.com')
eess.index(index='rudy', doc_type='test1', id=1, body=json.loads(content))

# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#
# #let's iterate over swapi people documents and index them
#
# r = requests.get('http://localhost:9200')
# i = 1
# while r.status_code == 200:
#     r = requests.get('http://swapi.co/api/people/'+ str(i))
#     es.index(index='sw', doc_type='people', id=i, body=json.loads(r.content))
#     i=i+1
#
# print(i)

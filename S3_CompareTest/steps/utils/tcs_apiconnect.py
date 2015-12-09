__author__ = 'rsantamaria'

import urllib
import urllib2
import json
import requests
import time

api_url = "http://tcs-api.dataagg-dev.moveaws.com/v1/search/"

api_data = '<TCService priority="9" ClientName="ORCA">' \
           '<Function>GetDataAggListings</Function>' \
           '<Login><password>Xe6aGAku</password><password>Xe6aGAku</password>' \
           '<UserAgent>Realtor/1.0</UserAgent><RetsUAPwd /></Login>' \
           '<Board BoardId="" ModuleId="1818" MetadataVersion="11241069" />' \
           '<Search BypassARAuthentication="1" OverrideRecordsLimit="9999999" IncludeCDATA="1" SelectPicFieldsOnly="0" ' \
           'ST_PublicListingStatus="A,O" ST_LastMod="09/22/2015T02:51:00-09/22/2015T21:52:43" ST_PicMod="" ResultFilePath="" /></TCService>'

api_posturl = api_url + api_data
# print api_posturl

api_post = requests.post(url=api_url, data=api_data).text
print api_post
# print api_post.text


api_poststatus = requests.get(api_url + api_post + "/status/").text
# print api_poststatus.text
print api_poststatus

for i in range(0,10):
# while api_poststatus != 4:
    if api_poststatus != 4:
        time.sleep(5)
    print api_post
    print api_poststatus
    if api_poststatus == 4:
        break

    continue

print api_poststatus

api_get = requests.get(api_url + api_post)
print api_get
print api_get.text
print api_get.status_code

# r2.close()


#print(req_open)

#get_result = apiurl+req_open
#print(get_result)

# r2 = requests.request("get", get_result)
# print r2

#r = requests.get(get_result)
#print r.status_code


# def tcs_results(params):


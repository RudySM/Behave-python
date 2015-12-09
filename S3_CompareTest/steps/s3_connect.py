__author__ = 'rsantamaria'

import boto
from boto.s3.key import Key


############################## GETS CONNECTION TO BUCKET
#def bucket_conn():
c = boto.connect_s3()
b = c.get_bucket('aggregation-workflow-test') # substitute your bucket name here


############################## GETS KEY AND FILE LOCATION
#def bucket_contents():
k = Key(b)
k.key = 'test/listing_test.txt'
list_contents = k.get_contents_as_string()

a = Key(b)
a.key = 'test/listingattribute_test.txt'
attr_contents = a.get_contents_as_string()


############################## gets count of records

cc_list = list_contents.splitlines()
#print(len(cc_list))  ### count of lines

cc_attr = attr_contents.splitlines()
#print(len(cc_attr))

############################## gets listing id's from listing file

listing_id = list()
for line in cc_list:
    x = line.split("!@!")
    listing_id.append(x[4])
    # print(x[4])

# print(listing_id)

# if (cmp(cc.sort(), listing_id.sort())) == 0:
#     print("The lists match")


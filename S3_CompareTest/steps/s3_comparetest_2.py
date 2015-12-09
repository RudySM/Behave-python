from behave import *
from swf_invoke import SWF_Invoke
import mysql
import s3_connect
from boto.s3.key import Key
from hamcrest import assert_that, equal_to, not_, not_none, is_, instance_of, greater_than, less_than_or_equal_to, is_in

use_step_matcher("re")



@given('connect to database: (?P<dev_xmart>.+)')
def step_impl(context, dev_xmart):
    """
    :type context behave.runner.Context
    """
    print("\n **** Scenario 2 **** \n\n")

    context.dev_xmart = mysql.dev_mart




@step("there are (?P<dup_feed_id>.+) in the feed file: (?P<dup_file>.+)")
def step_impl(context, dup_feed_id, dup_file):
    """
    :type context behave.runner.Context
    """
    dup = Key(s3_connect.b)
    dup.key = dup_file
    dup_contents = dup.get_contents_as_string()
    dup_contents = dup_contents.splitlines()

    dup_list_id = list()
    dup_counts = dict()

    for line in dup_contents:
        r = line.split("!@!")
        dup_list_id.append(r[4])

    for item in dup_list_id:
        dup_counts[item] = dup_counts.get(item, 0) + 1

    context.dup_feed_id = list()
    cnt = 0

    for k,v in dup_counts.items():
        if v > 1:
            context.dup_feed_id.append(k)
            cnt = cnt + 1

    context.dup_feed_id.sort()
    # print("=== Feed file duplicates: ", context.dup_feed_id, "\n")

    assert_that(cnt, greater_than(0))




@step("the Exceptions table is empty for IDs: (?P<dup_id1>.+), (?P<dup_id2>.+)")
def step_impl(context, dup_id1, dup_id2):
    """
    :type context behave.runner.Context
    """

    del_query = "delete from xmart.load_exception where sourcelistingid in (%s, %s);" % (dup_id1, dup_id2)
    del_exec = mysql.execute(context.dev_xmart, del_query)
    mysql.close_connection(context.dev_xmart)




@when('the SWF workflow is (?P<workflow>.+) with config file: (?P<configfilename>.+)')
def step_impl(context, workflow, configfilename):
    """
    :type context behave.runner.Context
    """

    wf = SWF_Invoke().run(configfilename)
    if wf != 'COMPLETED':
        print("Workflow did not complete... Status: ", wf, "\n")

    assert_that(wf, is_("COMPLETED"))
    # print("Workflow completed successfully. \n")



@then("Listing IDs that appear more than once in the feed file should appear in the Exceptions table")
def step_impl(context):
    """
    :type context behave.runner.Context
    """

    exception = mysql.select(context.dev_xmart, "select distinct sourcelistingid from xmart.load_exception;")
    mysql.close_connection(con=context.dev_xmart)

    context.id_exception = list()

    for row in exception:
        row = str(row)
        first_quot = row.find("'")
        comma = row.find(",")
        context.id_exception.append(row[first_quot+1:comma-1])

    context.id_exception.sort()
    # print("=== ID's from Exceptions table: ", context.id_exception, "\n")

    assert_that(context.id_exception, equal_to(context.dup_feed_id))

    print("Assertion complete (1 of 3). Duplicate ID's from the Feed File have been found in the Exceptions table. \n")




@step("Listing IDs in the Exceptions table should be present only once in the Stg Listing table")
def step_impl(context):
    """
    :type context behave.runner.Context
    """

    stg_list_cnts = dict()
    stg_list = list()

    stg = mysql.select(context.dev_xmart, "select sourcelistingid from xmart.stg_listing;")
    mysql.close_connection(con=context.dev_xmart)

    for row in stg:
        row = str(row)
        # print("========", id)
        first_quot = row.find("'")
        comma = row.find(",")
        stg_list.append(row[first_quot+1:comma-1])

    for aa in context.id_exception:
        assert_that(aa, is_in(stg_list))
        # if aa in stg_list:
        #     print(aa)

    for bb in stg_list:
        stg_list_cnts[bb] = stg_list_cnts.get(bb, 0) + 1
    # print(stg_list_cnts)

    for key,val in stg_list_cnts.items():
        assert_that(val, is_(1))

    print("Assertion complete (2 of 3). Listings that are in the Exceptions table appear only once in Stg table. \n")




@then("there should be No duplicate Listing ID's in the permanent Listing table")
def step_impl(context):
    """
    :type context behave.runner.Context
    """

    perm_list = list()
    perm_cnts = dict()

    perm = mysql.select(context.dev_xmart, "select sourcelistingid from xmart.listing;")
    mysql.close_connection(context.dev_xmart)

    for listid in perm:
        listid = str(listid)
        first_quot = listid.find("'")
        comma = listid.find(",")
        perm_list.append(listid[first_quot+1:comma-1])

    for li in perm_list:
        perm_cnts[li] = perm_cnts.get(li, 0) + 1

    # print(perm_cnts)
    for key,val in perm_cnts.items():
        assert_that(val, is_(1))

    print("Assertion complete (3 of 3). Listing Table contains only unique source listing id's. \n")


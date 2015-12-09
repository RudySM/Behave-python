
from behave import *
from swf_invoke import SWF_Invoke
import mysql
import s3_connect

from hamcrest import assert_that, equal_to, not_, not_none, is_, instance_of, greater_than, less_than_or_equal_to, is_in
use_step_matcher("re")




@given('connect to database: "(?P<url>.+com)" "(?P<username>.+)" "(?P<password>.+)" "(?P<database>.+)"')
def step_impl(context, url, username, password, database):
    """
    :type context behave.runner.Context
    """
    print("\n **** Scenario 1 **** \n\n")

    context.conn = mysql.connect(url, username, password, database)



@step('if data is present, database is cleared: "(?P<stg_attr_del>.+)" "(?P<stg_list_del>.+)" "(?P<perm_attr_del>.+)" "(?P<perm_list_del>.+)"')
def step_impl(context, stg_attr_del, stg_list_del, perm_attr_del, perm_list_del):
    """
    :type context behave.runner.Context
    """
    cur_count = str(mysql.select(context.conn, "Select count(*) from xmart.listing;"))
    cur_count = cur_count[2]

    stg_attr_del = "delete from xmart.stg_listing_attribute_value;"
    stg_list_del = "delete from xmart.stg_listing;"
    perm_attr_del = "delete from xmart.listing_attribute_value;"
    perm_list_del = "delete from xmart.listing;"

    if int(cur_count) >= 1:
        mysql.execute(con=context.conn, statement= stg_attr_del)
        mysql.execute(con=context.conn, statement= stg_list_del)
        mysql.execute(con=context.conn, statement= perm_attr_del)
        mysql.execute(con=context.conn, statement= perm_list_del)


        mysql.close_connection(context.conn)

    else:
        mysql.close_connection(context.conn)
        pass




@when('workflow: (?P<initiate>.+) with config file: (?P<configfilename>.+)')
def step_impl(context, initiate, configfilename):
    """
    :type context behave.runner.Context
    """

    initiate = SWF_Invoke()
    x = initiate.run(configfilename)

    if x != 'COMPLETED':
        print("Workflow did not complete successfully... Status: ", x, "\n")
        # assert_that(x, is_("COMPLETED"))

    assert_that(str(x).lower(), is_("completed"))




@then('(?P<db_list_cnt>.+) should match the (?P<s3_list_cnt>.+)')
def step_impl(context, db_list_cnt, s3_list_cnt):
    """
    :type context behave.runner.Context
    """

    db_list_cnt = str(mysql.select(context.conn, "Select count(*) from xmart.listing;"))

    frst_range = db_list_cnt.find("(")
    second_range = db_list_cnt.find("L")
    db_list_cnt = int(db_list_cnt[frst_range +2 : second_range])

    s3_list_cnt = len(s3_connect.cc_list)

    assert_that(db_list_cnt, equal_to(s3_list_cnt))

    print("Assertion complete (1 of 3). Listing Counts Match between S3 input and Database. \n")
    # print("Input S3 File count: ", s3_list_cnt)



@step('(?P<db_attr_cnt>.+) should equal the (?P<s3_attr_cnt>.+)')
def step_impl(context, db_attr_cnt, s3_attr_cnt):
    """
    :type context behave.runner.Context
    """

    db_attr_cnt = str(mysql.select(context.conn, "Select count(*) from xmart.listing_attribute_value;"))

    first_range = db_attr_cnt.find("(")
    sec_range = db_attr_cnt.find("L")
    db_attr_cnt = int(db_attr_cnt[first_range +2 : sec_range])
    mysql.close_connection(context.conn)
    # print(db_attr_cnt)

    s3_attr_cnt = len(s3_connect.cc_attr)
    # print(s3_attr_cnt)

    assert_that(db_attr_cnt, equal_to(s3_attr_cnt))

    print("Assertion complete (2 of 3). Attribute Counts Match between S3 attributes file and Database. \n")
    # context._attr_cnt_assert = assert_that(db_attr_cnt, equal_to(s3_attr_cnt))



@step('the (?P<mart_ids>.+) from mart will match the (?P<s3_ids>.+) from S3')
def step_impl(context, mart_ids, s3_ids):
    """
    :type context behave.runner.Context
    """

    mart_ids = list()
    m_ids = mysql.select(context.conn, "select * from xmart.listing")
    for line in m_ids:
        mart_ids.append(line[3])

    mart_ids.sort()
    # print(mart_ids)
    mysql.close_connection(context.conn)

    s3_ids = s3_connect.listing_id
    s3_ids.sort()
    # print(s3_ids, "\n")

    assert_that(mart_ids, equal_to(s3_ids))

    print("Assertion complete (3 of 3). Source Listing ID's Match between S3 feed file and the DB listing table. \n")

from behave import *
import mysql
from swf_invoke import SWF_Invoke
import s3_connect
from hamcrest import *
import re
use_step_matcher("re")
import time


@given("there is an agent and office Not already present in the roster tables: (?P<agent_test1>.+), (?P<agent_test2>.+): (?P<office_test1>.+), (?P<office_test2>.+)")
def step_impl(context, agent_test1, agent_test2, office_test1, office_test2):
    """
    :type context behave.runner.Context
    """
    print("\n **** Scenario 3 **** \n\n")

    context.agent_test1 = agent_test1
    context.agent_test2 = agent_test2
    context.office_test1 = office_test1
    context.office_test2 = office_test2

    del_agent = "delete from xmart.agent where sourceagentid in ('%s', '%s');" % (agent_test1, agent_test2)
    da = mysql.execute(mysql.dev_mart, del_agent)

    del_office = "delete from xmart.office where sourceofficeid in ('%s', '%s');" % (office_test1, office_test2)
    do = mysql.execute(mysql.dev_mart, del_office)

    mysql.close_connection(mysql.dev_mart)





@step("those records are saved: (?P<new_agent>.+), (?P<new_office>.+)")
def step_impl(context, new_agent, new_office):
    """
    :type context behave.runner.Context
    """

    context.new_agent = list()
    context.new_office = list()
    context.new_agent.extend([context.agent_test1, context.agent_test2])
    context.new_office.extend([context.office_test1, context.office_test2])


    context.s3_file = 'test/listing_test_dup.txt'

    # s3_buck = s3_connect.boto.connect_s3().get_bucket('aggregation-workflow-test')
    # s3_key = s3_connect.Key(s3_buck)
    # s3_key.key = context.s3_file
    # s3_content = s3_key.get_contents_as_string()
    # s3_list = s3_content.splitlines()
    #
    # s3_agtlist = list()
    # s3_offlist = list()
    # for line in s3_list:
    #     x = line.split("!@!")
    #     s3_agtlist.append(x[7])
    #     s3_offlist.append(x[8])

    # all_rost = list()

    # context.new_agent = list()
    # for agent in s3_agtlist:
    #     agt_query = "select sourceagentid from xmart.agent where sourceagentid = '%s';" % agent
    #     agt_select = mysql.select(mysql.dev_mart, agt_query)
    #
    #     if agt_select == '()':
    #         context.new_agent.append(agent)
    #         all_rost.append(agent)
    # print(context.new_agent)


    # context.new_office = list()
    # for off in s3_offlist:
    #     off_query = "select sourceofficeid from xmart.office where sourceofficeid = '%s';" % off
    #     off_select = str(mysql.select(mysql.dev_mart, off_query))
    #
    #     if qq == '()':
    #         context.new_office.append(off)
    #         all_rost.append(off)
    # # print(context.new_office)


    # if len(all_rost) == 0:
    #     print("No New Agents or Offices found in the feed file. \n")
    # assert_that(len(all_rost), greater_than_or_equal_to(1))





@when("the (?P<workflow>.+) is initiated for listing data with config file: (?P<configfilename>.+)")
def step_impl(context, workflow, configfilename):
    """
    :type context behave.runner.Context
    """
    workflow = SWF_Invoke().run(configfilename)

    if workflow != 'COMPLETED':
        print("The workflow failed to complete successfully. \n"
              "Ending status = ", workflow, "\n")

    assert_that(workflow, is_("COMPLETED"))

    # print("\nWorklfow completed successfully. \n\n")





@then("a new agent should appear in the Agent table")
def step_impl(context):
    """
    :type context behave.runner.Context
    """
    n_agent = list()

    notfound = 0

    for agent in context.new_agent:
        agt_query = "select sourceagentid from xmart.agent nolock where sourceagentid = '%s';" % agent
        agt_select = mysql.select(mysql.dev_mart, agt_query)
        mysql.close_connection(mysql.dev_mart)

        n_agent.append(agent)

        if str(agt_select) == '()':
            print("** Fail. Agent not found: ", agent)
            notfound = notfound + 1

    assert_that(notfound, is_(0))

    print("Assertion complete (1 of 2). New Agent(s) appear in Agent Table... ID's:", n_agent, "\n")





@step("a new office should appear in the Office table")
def step_impl(context):
    """
    :type context behave.runner.Context
    """
    n_office = list()

    notfound = 0

    for office in context.new_office:
        off_query = "select sourceofficeid from xmart.office nolock where sourceofficeid = '%s';" % office
        off_select = mysql.select(mysql.dev_mart, off_query)
        mysql.close_connection(mysql.dev_mart)

        n_office.append(office)

        if str(off_select) == '()':
            print("** Fail. Office not found: ", office)
            notfound = notfound + 1

    assert_that(notfound, is_(0))

    print("Assertion complete (2 of 2). New Office(s) appear in Office Table... ID's:", n_office, "\n")


    # office_result = re.findall("'(.+)'", str(off_select))


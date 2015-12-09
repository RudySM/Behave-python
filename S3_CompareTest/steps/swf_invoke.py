__author__ = 'rsantamaria'

import boto.swf.layer2 as swf
import boto.swf
import sys
import boto.exception
import json
import _json


class SWF_Invoke():

    def find_region(self, reg_name):
        if reg_name:
            for r in boto.swf.regions():
                if r.name == reg_name:
                    return r
        return None

    def run(self, configfile):
        try:
            region = self.find_region('us-west-2')

            workflowparams = '[\"[Ljava.lang.Object;\",[[\"com.realtor.gator.aws.workflow.ExecutionContext\",{\"config\":[\"java.util.Properties\",{\"manifest\":\"s3:aggregation-workflow-test//test/%s\",\"alpha\":\"beta\"}],\"memento\":[\"java.util.Properties\",{}]}]]]' % configfile

            out = swf.WorkflowType(region=region,
                                   domain='aggregation_workflow_test',
                                   name = 'Aggregate',
                                   version = '1.0',
                                   task_list='aggregation_workflow'
                                   )
            execution = out.start(workflow_id='Agg_Test_123', input=workflowparams)
                                  # input='[\"[Ljava.lang.Object;\",[[\"com.realtor.gator.aws.workflow.ExecutionContext\",{\"datasourceId\":\"ASDF\",\"type\":\"listing\",\"subType\":\"incremental\",\"config\":[\"java.util.Properties\",{\"manifest\":\"s3:aggregation-workflow-test//test/listing_test_config.xml\",\"alpha\":\"beta\"}],\"memento\":[\"java.util.Properties\",{}]}]]]') #% configpath
                                # input='[\"[Ljava.lang.Object;\",[[\"com.realtor.gator.aws.workflow.ExecutionContext\",{\"datasourceId\":\"ASDF\",\"type\":\"listing\",\"subType\":\"incremental\",\"config\":[\"java.util.Properties\",{\"one\":\"two\",\"alpha\":\"beta\"}],\"memento\":[\"java.util.Properties\",{}]}]]]')


            workflowid = execution.workflowId
            # print(workflowid)

            runid = execution.runId

            ### this poll waits a minimum of 60 seconds... too long. going with loop approach
            # t = out._swf.poll_for_activity_task('aggregation_workflow_test', 'aggregation_workflow')

            while True:
                xx = out._swf.describe_workflow_execution(domain='aggregation_workflow_test', run_id=runid, workflow_id=workflowid)
                rr = xx["executionInfo"][u'executionStatus']
                if rr == 'OPEN':
                    continue
                else:
                    break

            xx = out._swf.describe_workflow_execution(domain='aggregation_workflow_test', run_id=runid, workflow_id=workflowid)
            clos_status = xx["executionInfo"][u'closeStatus']

            return clos_status


            # yy = out._swf.get_workflow_execution_history(domain='aggregation_workflow_test', workflow_id=workflowid, run_id=runid)
            # print(yy)


        except:
            print("Unexpected error:" + str(sys.exc_info()[0]))


# i = SWF_Invoke()
# path = "listing_test_config.xml"
# i.run(path)

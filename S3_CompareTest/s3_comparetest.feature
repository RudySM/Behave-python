# Created by rsantamaria at 9/29/15

Feature: S3 to Aurora db



  Scenario: Workflow test: Compare Listing ID's and Counts
    # aggregation-workflow-test /  test/listing_test.txt
    Given connect to database: "xmart.dataagg-dev.moveaws.com" "dataagg" "UWm6WggX" "xmart"
    And if data is present, database is cleared: "Delete Stg Listing Attribute" "Delete Stg Listing" "Delete Listing Attribute" "Delete Listing"
    When workflow: initiate with config file: listing_test_config.xml
    Then database listing count should match the S3 listing count
    And database attribute count should equal the S3 attribute count
    And the Listing ID's from mart will match the Listing ID's from S3



  Scenario: Test for Duplcates Within the Feed File
    # aggregation-workflow-test /  test/listing_test_dup.txt
    Given connect to database: Dev xMart
    And there are duplicate Listing ID's in the feed file: test/listing_test_dup.txt
    And the Exceptions table is empty for IDs: 21202652, 21210009
    When the SWF workflow is kicked off with config file: listing_test_dup_config.xml
    Then Listing IDs that appear more than once in the feed file should appear in the Exceptions table
    And Listing IDs in the Exceptions table should be present only once in the Stg Listing table
    And there should be No duplicate Listing ID's in the permanent Listing table


  @skip
  Scenario: Verify that a skeleton Roster is created in Agent and Office tables from a Listing File
    # aggregation-workflow-test /  test/listing_test_dup.txt
    Given there is an agent and office Not already present in the roster tables: agent_test, 3115 : office_test, 6105
    And those records are saved: new_agent, new_office
    When the workflow is initiated for listing data with config file: listing_test_dup_config.xml
    Then a new agent should appear in the Agent table
    And a new office should appear in the Office table


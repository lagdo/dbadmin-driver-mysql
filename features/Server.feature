Feature: Database server
  As a database user, I want to connect to a database server

  Scenario: Read the database names in version lower than 5
    Given I am connected to the default server
    And The driver version is 4.9
    When I get the database list
    Then The show databases query is executed

  Scenario: Read the database names in version greater than 5
    Given I am connected to the default server
    And The driver version is 5.5
    When I get the database list
    Then The select schema name query is executed

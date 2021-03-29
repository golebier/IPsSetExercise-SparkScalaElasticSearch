# IPsSetExercise-SparkScalaElasticSearch

## Goal:

Write an algorithm that process a given set of ip ranges by removing their intersections and returning a set of mutually exclusive ip ranges.

## Example:

## Sample Input:

197.203.0.0, 197.206.9.255

197.204.0.0, 197.204.0.24

201.233.7.160, 201.233.7.168

201.233.7.164, 201.233.7.168

201.233.7.167, 201.233.7.167

203.133.0.0, 203.133.255.255

## Sample Output:

197.203.0.0, 197.203.255.255

197.204.0.25, 197.206.9.255

201.233.7.160, 201.233.7.163

203.133.0.0, 203.133.255.255

## Nice to have:

- Done: A short description of how to run it
- NotDone: Generator for input test data
- Done: Store and fetch Input from relational database
- Done: Save results in Elasticsearch
- Done: Utilize docker-compose for localhost setup

## A short description, how to run the project:

- git clone ${PROJECT}

- mvn clean install

- docker-compose up

- docker-compose down

## Description/Results/What we can observe:

- above commands will take you from the git repo till the reults you can read in the ES

- the Spark/Scala code is simple, and I would suggest to start reading with tests

- we have only to high level tests, as the code is simple, and the Spark logic I could develop with single job

- I've started with BaseLogicDiscoveryACTest, as a discovery place where I could verify how to approach the task and understand the task deeply

- ExtractMutuallyExclusiveRangesBaseACTest is the core test that mock JDBC connector and ElasticSearch writes, to focus on the Spark job and thest the job in the way as it runs on the a cluster

- ExtractMutuallyExclusiveRanges - the Spark job main

- ExtractMutuallyExclusiveRangesBase - box where we can easily test the job

- DbJdbcExtractor - anything we need to work with JDBC/DB

- ElasticSearchPusher - anything we need to work with ElasticSearch

- docker-compose.yml - a file that will run anything you need (presently doesn't work with some issues)

## Simplification:

I've used simpler ranges approach:

input:

      0, 50
      20, 40
      52, 55
      60, 68
      64, 68
      67, 67

expected:

    0, 19
    41, 50
    52, 55
    60, 63

the ranges give the same rule as for IP, yet for the IP the windowing would need to change as it: 0-255.
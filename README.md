# IPsSetExercise-SparkScalaElasticSearch

## Goal:

Write an algorithm that process a given set of ip ranges by removing their intersections and returning a set of mutually exclusive ip ranges.

## Example:

## Sample Input:

197.203.0.0, 197.206.9.255

197.204.0.0,197.204.0.24

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

- Generator for input test data
- Store and fetch Input from relational database
- Save results in Elasticsearch
- Utilize docker-compose for localhost setup

# Cloud-based-applications: Assignment 2

*Author: André Martins*  
*ID: 0230991223*

# Project design

All the execution of this small project is based on docker, more specifically in two separate containers. One container is responsible to run all the code (both for part one and part two of the project) and the other one contains a mongodb database used for all the mongodb operations.

Part one and part two of the project have their respective python scripts and both determine their execution time for only the queries that have been asked. Additionally both parts do execute all the three queries that have been requested for this assignment.

## Docker mounting points (volumes)

This project also contains two mounting points from the docker containers and the file system. These mounting points allow to easily visualize the output of the executed code ( all the files inside `output`) and gathering a consistent state in the mongodb database (all the files contained inside `mongo_data`)

# How to run the project

Inside the root of this project, you shall run the following commands:

```bash
docker compose build # building all the needed docker images and initializing both containers
docker compose up -d # the -d should be removed if you would like to directly see the result of the calculations 
```

After both containers have ran, you should be able to visualize the output of both ran scripts inside a `.txt` file inside the `/output` folder.

# Final results and conclusion

After running the project on my local machine, the results are the following:

```text
Part 1: Spark analysis
Execution time: 4.669925212860107

Part 2: MongoDB analysis
Execution time: 0.23716020584106445
```

## Small observation

As we can see the the mongodb operations ran ~20x faster compared the the spark operations.

## Interpretation

MongoDB executed queries ~20x faster than Spark for our 50,000-record dataset because:

- **Low overhead:** MongoDB's aggregation pipeline runs as compiled C++ code vs. Spark's JVM/ Python overhead

- **Index utilization:** MongoDB uses B-tree indexes for O(log n) lookups vs. Spark's O(n) scans

- **Architecture:** Spark is optimized for distributed computation across clusters, creating overhead for single-node execution

This demonstrates that the right tool depends on data scale: MongoDB excels for medium-sized datasets with indexed queries, while Spark's advantages emerge with much larger datasets or complex distributed processing needs.
# _Spark-Odyssey_
A journey in to the world of Machine Learning algorithms using Apache Spark.

## Prerequisites
- [Java](https://java.com/en/download/)
- [Gradle](https://gradle.org/)
- [Scala](https://www.scala-lang.org/)
- [Spark] (https://spark.apache.org/)

## Build and Demo process

### Clone the Repo
`git clone https://github.com/ramuramaiah/spark-odyssey.git`

### Build
`./gradlew clean jar`

## What the demo does?
The commands to run the Spark jobs are available in the batch files
For e.g. To run the collocation algorithm, the colloc.bat has the following entry

**colloc.bat**
```bat
%SPARK_HOME%/bin/spark-submit ^
    --class spark.odyssey.colloc.Driver ^
    --jars file:///C:/mahout/lib/mahout-math-0.13.0.jar ^
    --master local ^
    --deploy-mode client ^
    --driver-memory 4g ^
    --executor-memory 2g ^
    --executor-cores 1 ^
    --queue colloc ^
    build/libs/spark-odyssey.jar ^
    --algo g_2 ^
	-s 1 ^
    "./build/resources/main/input_events.csv" ^
    "./output"
```

## Libraries Included
- Spark - 2.1.0

## Useful Links
- [Spark Docs - Root Page](http://spark.apache.org/docs/latest/)
- [Spark Programming Guide](http://spark.apache.org/docs/latest/programming-guide.html)
- [Spark Latest API docs](http://spark.apache.org/docs/latest/api/)
- [Scala API Docs](http://www.scala-lang.org/api/2.12.1/scala/)
 
## Issues or Suggestions

- Raise one on github
- Send me a mail -> ramu.ramaiah@gmail.com

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
    "./build/resources/main/cooccur_inputs.csv" ^
    "./output"

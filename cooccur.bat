%SPARK_HOME%/bin/spark-submit ^
    --class spark.odyssey.cooccur.Driver ^
    --master local ^
    --deploy-mode client ^
    --driver-memory 4g ^
    --executor-memory 2g ^
    --executor-cores 1 ^
    --queue cooccur ^
    build/libs/spark-odyssey.jar ^
	-n 1 ^
	-min 1 ^
	-top 5 ^
    "./build/resources/main/cooccur_inputs.csv" ^
    "./output"

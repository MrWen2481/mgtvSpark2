芒果tv sdk

spark2-submit \
 --master yarn \
 --executor-memory 12g \
 --executor-cores 4 \
 --num-executors 10  \
 --driver-memory 3G \
 --conf spark.default.parallelism=500 \
 --conf spark.sql.shuffle.partitions=500 \
 --conf spark.executor.memoryOverhead=4096 \
 --class com.starv.yd.YDSdk /home/zyx/Starv-Spark2.jar 20180521 HNYD
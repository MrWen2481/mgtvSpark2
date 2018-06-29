芒果tv sdk

spark2-submit \
 --master yarn \
 --executor-memory 12g \
 --executor-cores 4 \
 --num-executors 10  \
 --driver-memory 3G \
 --conf spark.default.parallelism=500 \
 --conf spark.executor.memoryOverhead=4096 \
 --class com.starv.yd.YDSdk /home/zyx/Starv-Spark2.jar 20180521 HNYD
 
 
 spark2-submit \
  --master yarn \
  --executor-memory 12g \
  --executor-cores 4 \
  --num-executors 10  \
  --driver-memory 3G \
  --conf spark.default.parallelism=500 \
  --conf spark.executor.memoryOverhead=4096 \
  --class com.starv.dx.DxApk /home/public/starv/spark/Starv-Spark2.jar 20180601 HNDX
  
  
  /usr/bin/spark-submit \
   --master yarn \
   --class com.starv.mgtv.SaveToHbaseByMinuteNew \
   --jars /home/public/starv/lib/htrace-core.jar \
   --num-executors 12 \
   --executor-memory 8G \
   --executor-cores 4 \
   --driver-memory 3G  \
   --total-executor-cores 160 \
   --conf spark.default.parallelism=500 \
   --conf spark.storage.memoryFraction=0.5  \
   --conf spark.shuffle.memoryFraction=0.3  \
   --conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
   --conf spark.yarn.executor.memoryOverhead=4096 \
   /home/public/starv/spark/Starv-Spark.jar 20180601  HNDX
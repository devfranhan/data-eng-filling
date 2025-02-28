hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input hdfs://franhan-cluster/dados-1pb/ \
    -output hdfs://franhan-cluster/output/ \
    -mapper "python mapper.py" \
    -reducer "python reducer.py"

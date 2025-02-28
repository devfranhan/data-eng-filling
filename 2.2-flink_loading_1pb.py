from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(100)

data_stream = env.read_text_file("s3://franhan-bucket/dados-1pb/")

data_stream.map(lambda line: (line.split(",")[0], 1)).key_by(lambda x: x[0]).sum(1)

env.execute("Load 1PB Data")

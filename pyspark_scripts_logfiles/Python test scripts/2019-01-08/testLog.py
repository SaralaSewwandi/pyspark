from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, regexp_extract

conf = SparkConf().setAppName("MIFE Application").setMaster("local")
sc = SparkContext(conf=conf)


sqlContext = SQLContext(sc)

log_file = sqlContext.read.text( "hdfs://sandbox:9000/user/root/bl/carbon.log")

#log_file.show( 5 )
#log_file.show(5, truncate=False)
split_df = log_file.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                        regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status')
                        )


split_df.show(2, truncate=False)


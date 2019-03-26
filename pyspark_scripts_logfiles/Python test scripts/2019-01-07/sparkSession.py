from pyspark.sql import SQLContext
sqlContext=SQLContext(sc)

custDf=sqlContext.read.json(customers.json)
custDf.show()
custDf.printSchema()

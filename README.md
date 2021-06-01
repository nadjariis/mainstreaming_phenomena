# mainstreaming_phenomena
*Code and data for thesis by Nina Melin and Nadja Riis*

Find the clean data here:

https://drive.google.com/drive/folders/1hYDz1J-5i1SxgfXnov09Ys9u8sV-kGRk?usp=sharing

Data can be loaded into a pyspark dataframe, by defining the schema:

    schema = StructType([
        StructField("bigram", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("bigram_percent", FloatType(), True),
        StructField("bigram_percent_smooth", FloatType(), True)])
    
And then reading the data:

    df = spark.read.option('header', False).schema(schema).csv('/path/to/data')

from pyspark.sql import SparkSession
from pyspark import SparkConf
from schema import SchemaProvider
from pyspark.sql.functions import from_json, col
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    LongType,
    DoubleType,
)


def main():
    json_file_path = "./data/sample.json"

    conf = (
        SparkConf()
        .set("spark.executor.memory", "32g")
        .set("spark.driver.memory", "16g")
    )

    spark = (
        SparkSession.builder.appName("Read Json Example")
        .config(conf=conf)
        .getOrCreate()
    )

    schema_provider = SchemaProvider()
    schema = schema_provider.get_schema()

    in_network_schema = schema_provider.extract_schema("in_network", schema)

    #print(in_network_schema)

    df = spark.read.json(json_file_path, schema=schema, multiLine=True)

    # TO DO: Schema validation. Compare & validate

    # attempt to extract the in_network field

    df_parsed = df.select(col("in_network"))

    first_row = df_parsed.head()[0]

    #print(first_row[0])

    #in_network_schema = schema_provider.extract_schema("in_network", schema)


    single_record_schema = StructType([
                    StructField("billing_code", StringType(), True),
                    StructField("billing_code_type", StringType(), True),
                    StructField("billing_code_type_version", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("negotiated_rates", ArrayType(
                        StructType([
                            StructField("negotiated_prices", ArrayType(
                                StructType([
                                    StructField("billing_class", StringType(), True),
                                    StructField("billing_code_modifier", ArrayType(StringType(), True), True),
                                    StructField("expiration_date", StringType(), True),
                                    StructField("negotiated_rate", DoubleType(), True),
                                    StructField("negotiated_type", StringType(), True),
                                    StructField("service_code", ArrayType(StringType(), True), True)
                                ])
                            ), True),
                            StructField("provider_references", ArrayType(LongType(), True), True)
                        ])
                    ), True),
                    StructField("negotiation_arrangement", StringType(), True)
                ])

    df2 = spark.createDataFrame([first_row[0]], single_record_schema)

    df2.show(n=2)





if __name__ == "__main__":
    main()

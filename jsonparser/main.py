from pyspark.sql import SparkSession
from pyspark import SparkConf
from schema import SchemaProvider
from pyspark.sql.functions import explode, col, explode_outer, posexplode
from pyspark.sql.dataframe import DataFrame
from typing import Callable

Transformation = Callable[[DataFrame], DataFrame]

def parse_provider(df: DataFrame) -> DataFrame:
    exploded_provider_groups_df = df.select("reporting_entity_name", 
                                            "reporting_entity_type", 
                                            "last_updated_on",
                                            "version",
                                            "chunk_id",
                                            col("provider_references.provider_group_id").alias("provider_group_id"),
                                            explode("provider_references.provider_groups").alias("provider_groups"))


    exploded_npi_df = exploded_provider_groups_df.select("reporting_entity_name", 
                                                         "reporting_entity_type", 
                                                         "last_updated_on", 
                                                         "version",
                                                         "chunk_id",
                                                         "provider_group_id",
                                                         col("provider_groups.tin.type").alias("tin_type"),
                                                         col("provider_groups.tin.value").alias("tin_value"),
                                                         explode("provider_groups.npi").alias("npi"))
    

    flattened_df = exploded_npi_df.select("reporting_entity_name",
                                          "reporting_entity_type",
                                          "last_updated_on",
                                          "version",
                                          "chunk_id",
                                          "provider_group_id",
                                          "npi",
                                          "tin_type",
                                          "tin_value")

    return flattened_df
    


def parse_rates(df: DataFrame) -> DataFrame:
    # TO DO: Schema validation. Compare & validate
    exploded_negotiated_rates_df = df.select("reporting_entity_name", 
                                             "reporting_entity_type",
                                             col("in_network.name").alias("name"),
                                             col("in_network.billing_code").alias("billing_code"),
                                             col("in_network.billing_code_type").alias("billing_code_type"),
                                             col("in_network.billing_code_type_version").alias("billing_code_type_version"),
                                             col("in_network.description").alias("description"),
                                             col("in_network.negotiation_arrangement").alias("negotiation_arrangement"),
                                             explode("in_network.negotiated_rates").alias("negotiated_rates"))
    
    exploded_negotiated_prices_df = exploded_negotiated_rates_df.select("reporting_entity_name", 
                                                                        "reporting_entity_type", 
                                                                        "name", 
                                                                        "billing_code", 
                                                                        "billing_code_type",
                                                                        "billing_code_type_version", 
                                                                        "description", 
                                                                        "negotiation_arrangement",
                                                                        col("negotiated_rates.provider_references").alias("provider_references"),
                                                                        explode("negotiated_rates.negotiated_prices").alias("negotiated_prices"))
    
    exploded_provider_references = exploded_negotiated_prices_df.select("reporting_entity_name", 
                                                                        "reporting_entity_type", 
                                                                        "name", "billing_code", 
                                                                        "billing_code_type", 
                                                                        "billing_code_type_version",
                                                                        "description", 
                                                                        "negotiation_arrangement",
                                                                        col("negotiated_prices.billing_class").alias("billing_class"),
                                                                        col("negotiated_prices.expiration_date").alias("expiration_date"),
                                                                        col("negotiated_prices.negotiated_rate").alias("negotiated_rate"),
                                                                        col("negotiated_prices.negotiated_type").alias("negotiated_type"),
                                                                        col("negotiated_prices.service_code").alias("service_code"),
                                                                        explode("provider_references").alias("provider_references"))
    
    exploded_service_codes = exploded_provider_references.select("reporting_entity_name", 
                                                                 "reporting_entity_type", 
                                                                 "name", 
                                                                 "billing_code", 
                                                                 "billing_code_type",
                                                                 "billing_code_type_version",
                                                                 "description", 
                                                                 "negotiation_arrangement", 
                                                                 "billing_class",
                                                                 "expiration_date",
                                                                 "negotiated_rate",
                                                                 "negotiated_type",
                                                                   explode_outer("service_code"),"provider_references")
    
    return exploded_service_codes



def process_df(df: DataFrame, transformation: Transformation, chunk_size: int):

    df_with_chunks = df.withColumn("chunk_id", (col("pos") / chunk_size).cast("int"))
    chunk_ids = df_with_chunks.select("chunk_id").distinct().collect()

    for chunk in chunk_ids:
        chunk_id = chunk["chunk_id"]
        df_chunk = df_with_chunks.filter(col("chunk_id") == chunk_id).drop("pos")

        result = transformation(df_chunk)

        result.write.mode('overwrite').option("header", True).csv(f'./data/mini_sample_output/{transformation.__name__}/chunk_{chunk_id}/')


def main():
    
    json_file_path = "./data/mini_sample.json"

    conf: SparkConf = (
        SparkConf()
        .set("spark.executor.memory", "32g")
        .set("spark.driver.memory", "16g")
    )   

    spark: SparkSession = (
        SparkSession.builder
        .appName("Read Json Example")
        .config(conf=conf)
        .getOrCreate()
    )

    schema_provider = SchemaProvider()
    schema = schema_provider.get_schema()


    df: DataFrame = spark.read.json(json_file_path, schema=schema, multiLine=True) # TO DO schema checks and validations, handle erroneous/improperly structured files


    CHUNK_SIZE = 1

    TRANSFORMATIONS = {
        "parse_rates": parse_rates,
        "parse_provider": parse_provider
    }

    provider_index_df = df.select("reporting_entity_name", 
                              "reporting_entity_type",
                              "last_updated_on", 
                              "version",
                              posexplode(col("provider_references")).alias("pos", "provider_references")
                              )
      

    in_network_index_df = df.select("reporting_entity_name", 
                                       "reporting_entity_type", 
                                       posexplode("in_network").alias("pos","in_network")
                                       )


    process_df(in_network_index_df, parse_rates, CHUNK_SIZE)





## TO DO: 1. Build feature that will take in a parameter (chunk size), and split the array into that many chunks
## TO DO: 2. Build out the logic for performing the necessary explosions/transformations for the providers and pricing
## TO DO: 3. implement threading: Each one of these chunks will then be passed to a later worker node that will intake 2 
## parameters (data, operation) which will then process the chunk by performing the passed operation
## TO DO: 4. Create tests to validate the exploded schemas are the expected transformation

if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark import SparkConf
from schema import SchemaProvider
from pyspark.sql.functions import explode, col, expr, explode_outer, posexplode
from pyspark.sql import Row

def parse_provider_chunk(chunk_id, chunk_df):

    # exploded_provider_references_df = chunk_df.select("reporting_entity_name", 
    #                                             "reporting_entity_type", 
    #                                             "last_updated_on", 
    #                                             "version",
    #                                             "chunk_id",
    #                                             explode("provider_references.").alias("provider_references"))

    exploded_provider_groups_df = chunk_df.select("reporting_entity_name", 
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
    

    flattened_df = exploded_npi_df.select(
        "reporting_entity_name",
        "reporting_entity_type",
        "last_updated_on",
        "version",
        "chunk_id",
        "provider_group_id",
        "npi",
        "tin_type",
        "tin_value"
    )

    flattened_df.write.mode('overwrite').option("header", True).csv(f'./data/mini_sample_output/providers/chunk_{chunk_id}/')


def parse_rates(chunk_id, chunk_df):
    # TO DO: Schema validation. Compare & validate
    # exploded_in_network_df = df.select("reporting_entity_name", "reporting_entity_type", explode("in_network").alias("in_network"))

    exploded_negotiated_rates_df = chunk_df.select("reporting_entity_name", "reporting_entity_type",
                                                             col("in_network.name").alias("name"),
                                                             col("in_network.billing_code").alias("billing_code"),
                                                             col("in_network.billing_code_type").alias("billing_code_type"),
                                                             col("in_network.billing_code_type_version").alias("billing_code_type_version"),
                                                             col("in_network.description").alias("description"),
                                                             col("in_network.negotiation_arrangement").alias("negotiation_arrangement"),
                                                             explode("in_network.negotiated_rates").alias("negotiated_rates"))
    
    exploded_negotiated_prices_df = exploded_negotiated_rates_df.select("reporting_entity_name", "reporting_entity_type", "name", "billing_code", "billing_code_type",
                                                                        "billing_code_type_version", "description", "negotiation_arrangement",
                                                                        col("negotiated_rates.provider_references").alias("provider_references"),
                                                                        explode("negotiated_rates.negotiated_prices").alias("negotiated_prices"))
    
    exploded_provider_references = exploded_negotiated_prices_df.select("reporting_entity_name", "reporting_entity_type", "name", "billing_code", "billing_code_type", "billing_code_type_version",
                                                    "description", "negotiation_arrangement",
                                                    col("negotiated_prices.billing_class").alias("billing_class"),
                                                    col("negotiated_prices.expiration_date").alias("expiration_date"),
                                                    col("negotiated_prices.negotiated_rate").alias("negotiated_rate"),
                                                    col("negotiated_prices.negotiated_type").alias("negotiated_type"),
                                                    col("negotiated_prices.service_code").alias("service_code"),
                                                    explode("provider_references").alias("provider_references"))
    
    exploded_service_codes = exploded_provider_references.select("reporting_entity_name", "reporting_entity_type", "name", "billing_code", "billing_code_type", "billing_code_type_version",
                                                "description", "negotiation_arrangement", "billing_class","expiration_date",
                                                "negotiated_rate","negotiated_type", explode_outer("service_code"),"provider_references")

    exploded_service_codes.write.mode('overwrite').option("header", True).csv(f'./data/mini_sample_output/rates/chunk_{chunk_id}/')

def main():
    
    json_file_path = "./data/mini_sample.json"

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


    df = spark.read.json(json_file_path, schema=schema, multiLine=True) # TO DO schema checks and validations, handle erroneous/improperly structured files


    CHUNK_SIZE = 1

    # df_with_index = df.select("reporting_entity_name", 
    #                           "reporting_entity_type",
    #                           "last_updated_on", 
    #                           "version",
    #                           posexplode(col("provider_references")).alias("pos", "provider_references")
    #                           )
    
    # df_with_index = df_with_index.withColumn("chunk_id", (col("pos") / CHUNK_SIZE).cast("int"))
    
    # chunk_ids = df_with_index.select("chunk_id").distinct().collect()
    
    # df_with_index.show(n=5)

    # for chunk in chunk_ids:
    #     chunk_id = chunk["chunk_id"]
    #     chunk_df = df_with_index.filter(col("chunk_id") == chunk_id).drop("pos")
    #     chunk_df.show(n=5)
    #     parse_provider_chunk(chunk_id, chunk_df)
    


    exploded_in_network_df = df.select("reporting_entity_name", "reporting_entity_type", posexplode("in_network").alias("pos","in_network"))


    exploded_in_network_with_index = exploded_in_network_df.withColumn("chunk_id", (col("pos") / CHUNK_SIZE).cast("int"))

    exploded_in_network_with_index.show(n=5)

    chunk_ids = exploded_in_network_with_index.select("chunk_id").distinct().collect()

    for chunk in chunk_ids:
        chunk_id = chunk["chunk_id"]
        chunk_df = exploded_in_network_with_index.filter(col("chunk_id") == chunk_id).drop("pos")
        chunk_df.show(n=5)
        parse_rates(chunk_id, chunk_df)





## TO DO: 1. Build feature that will take in a parameter (chunk size), and split the array into that many chunks
## TO DO: 2. Build out the logic for performing the necessary explosions/transformations for the providers and pricing
## TO DO: 3. implement threading: Each one of these chunks will then be passed to a later worker node that will intake 2 
## parameters (data, operation) which will then process the chunk by performing the passed operation
## TO DO: 4. Create tests to validate the exploded schemas are the expected transformation

if __name__ == "__main__":
    main()

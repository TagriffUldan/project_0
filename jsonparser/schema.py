from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    LongType,
    DoubleType,
    DataType,
)


class SchemaProvider:

    @staticmethod
    def get_schema() -> StructType:
        schema = StructType([
            StructField("in_network", ArrayType(
                StructType([
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
            ), True),
            StructField("last_updated_on", StringType(), True),
            StructField("provider_references", ArrayType(
                StructType([
                    StructField("provider_group_id", LongType(), True),
                    StructField("provider_groups", ArrayType(
                        StructType([
                            StructField("npi", ArrayType(LongType(), True), True),
                            StructField("tin", StructType([
                                StructField("type", StringType(), True),
                                StructField("value", StringType(), True)
                            ]), True)
                        ])
                    ), True)
                ])
            ), True),
            StructField("reporting_entity_name", StringType(), True),
            StructField("reporting_entity_type", StringType(), True),
            StructField("version", StringType(), True)
        ])

        return schema

    def extract_schema(self, field_name: str, schema: DataType) -> None | DataType:
        if isinstance(schema, StructType):
            for field in schema.fields:
                if field.name == field_name:
                    return field.dataType
                else:
                    result = self.extract_schema(field_name, field.dataType)
                    if result is not None:
                        return result
        elif isinstance(schema, ArrayType) and isinstance(
            schema.elementType, StructType
        ):
            return self.extract_schema(field_name, schema.elementType)
        return None

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, regexp_replace

spark = SparkSession.builder.getOrCreate()

airbnb_filtered_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("sep", ",")
    .option("mode", "DROPMALFORMED")
    .csv("../la_dataset/listings.csv")
    .select(
        col("id"),
        col("neighbourhood_cleansed"),
        col("neighbourhood_group_cleansed"),
        col("latitude"),
        col("longitude"),
        col("property_type"),
        col("room_type"),
        col("price"),
        col("review_scores_rating"),
        col("number_of_reviews"),
        col("accommodates"),
        col("bedrooms"),
        col("beds")
    )
    .withColumn("latitude", col("latitude").cast("float"))
    .withColumn("longitude", col("longitude").cast("float"))
    .withColumn("price", regexp_replace(col("price"), "[$,]", "").cast("float"))
    .withColumn("review_scores_rating", col("review_scores_rating").cast("float"))
    .withColumn("number_of_reviews", col("number_of_reviews").cast("int"))
    .withColumn("accommodates", col("accommodates").cast("int"))
    .withColumn("bedrooms", col("bedrooms").cast("int"))
    .withColumn("beds", col("beds").cast("int"))
)

# Define numerical and categorical columns
numerical_columns = ["price", "review_scores_rating", "number_of_reviews", "accommodates", "bedrooms", "beds"]
categorical_columns = ["id", "neighbourhood_cleansed", "neighbourhood_group_cleansed", "property_type", "room_type"]

def show_weird_values(df, categorical_columns):
    regex_pattern = r"^(\d+|\d+\.\d+|\".*\"|'.*'|\[.*\])$"

    for col_name in categorical_columns:
        print(f"Weird distinct values for {col_name}:")
        df.select(col_name).distinct().filter(col(col_name).rlike(regex_pattern)).show(truncate=False)

def count_negative_values(df, numerical_columns):
    print("Negative values count in numerical columns:")
    df.select(
        [sum((col(c) < 0).cast("int")).alias(c) for c in numerical_columns]
    ).show()

def count_missing_values(df, numerical_columns, categorical_columns):
    # Count NULL values for numerical columns
    missing_numerical = df.select(
        [sum(col(c).isNull().cast("int")).alias(c) for c in numerical_columns]
    )

    # Count NULL values for categorical columns
    missing_categorical = df.select(
        [sum(col(c).isNull().cast("int")).alias(c) for c in categorical_columns]
    )

    # Show results
    print("Missing values in numerical columns:")
    missing_numerical.show()

    print("Missing values in categorical columns:")
    missing_categorical.show()

def detect_weird_airbnb_listings(df):
    # Identify Listings Where Bedrooms > Accommodates
    bedrooms_exceed_accommodates = df.filter(col("bedrooms") > col("accommodates")).select(
        "neighbourhood_cleansed", "bedrooms", "beds", "accommodates", "price"
    )

    # Identify Listings Where Beds > Accommodates
    beds_exceed_accommodates = df.filter(col("beds") > col("accommodates")).select(
        "neighbourhood_cleansed", "bedrooms", "beds", "accommodates", "price"
    )

    # Identify Listings with Extremely High Bedrooms (>10)
    outlier_bedrooms = df.filter(col("bedrooms") > 10).select(
        "neighbourhood_cleansed", "bedrooms", "beds", "accommodates", "price"
    )

    # Identify Listings with Extremely High Beds (>15)
    outlier_beds = df.filter(col("beds") > 15).select(
        "neighbourhood_cleansed", "bedrooms", "beds", "accommodates", "price"
    )

    # Show Sample Listings
    print("Sample Listings Where Bedrooms > Accommodates:")
    bedrooms_exceed_accommodates.show(10, truncate=False)

    print("Sample Listings Where Beds > Accommodates:")
    beds_exceed_accommodates.show(10, truncate=False)

    print("Sample Listings With Extreme Bedrooms (>10):")
    outlier_bedrooms.show(10, truncate=False)

    print("Sample Listings With Extreme Beds (>15):")
    outlier_beds.show(10, truncate=False)

def filter_data(df, categorical_columns):
    # Replace NULL values with 0
    df = df.fillna({"bedrooms": 0, "beds": 0})
    df = df.withColumn("is_rated", when(col("review_scores_rating").isNotNull(), 1).otherwise(0))
    df = df.filter(col("price").isNotNull())

    # Handle weird property type
    df = df.filter(
        (col("property_type") != "Tipi") & 
        (~col("property_type").startswith("Shepherd"))
    )
    
    # Replace NULL values with "N/A"
    df = df.fillna("N/A", subset=categorical_columns)

    # Drop invalid lat and long values
    df = df.filter(
        (col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180))
    )

    #Handle duplicates
    df = df.dropDuplicates(["neighbourhood_cleansed", "latitude", "longitude", "price"])
    
    return df

#count_negative_values(airbnb_filtered_df, numerical_columns)
#count_missing_values(airbnb_filtered_df, numerical_columns, categorical_columns)
#show_weird_values(airbnb_filtered_df, categorical_columns)
#detect_weird_airbnb_listings(airbnb_df)

airbnb_filtered_df = filter_data(airbnb_filtered_df, categorical_columns)
airbnb_filtered_df.write \
    .option("maxRecordsPerFile", 100000) \
    .mode("overwrite") \
    .parquet("../la_dataset/cleaned_listings")


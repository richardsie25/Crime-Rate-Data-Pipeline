
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, lit, to_timestamp, trim, min, max, regexp_extract

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col, to_timestamp, trim

crime_filtered_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("sep", ",")
    .option("mode", "DROPMALFORMED")
    .csv("../la_dataset/LA_crimedata.csv")
    .select(
        col("DR_NO"),
        col("Date Rptd"),
        col("DATE OCC"),
        col("Crm Cd"),
        col("Crm Cd Desc"),
        col("LAT"),
        col("LON")
    )
    .withColumn("DR_NO", col("DR_NO").cast("int"))
    .withColumn("Date Rptd", to_timestamp(trim(col("Date Rptd")), "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("DATE OCC", to_timestamp(trim(col("DATE OCC")), "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("Crm Cd", col("Crm Cd").cast("int"))
    .withColumn("LAT", col("LAT").cast("float"))
    .withColumn("LON", col("LON").cast("float"))
).filter(col("DATE OCC") >= to_timestamp(lit("2024-01-01"), "yyyy-MM-dd"))

violent_crimes = r"(?i)\b(assault|battery|homicide|manslaughter|kidnapping|threat|stalking|lynching)\b"
sex_crimes = r"(?i)\b(rape|sexual|sodomy|lascivious|lewd|pornography|indecent|molestation|pimping|pandering)\b"
property_crimes = r"(?i)\b(theft|burglary|robbery|embezzlement|fraud|shoplifting|stolen|vandalism|identity)\b"
public_order_crimes = r"(?i)\b(firearm|weapon|riot|arson|bomb|discharge|trespassing|resisting|inciting)\b"

crime_filtered_df = crime_filtered_df.withColumn(
    "Crime Category",
    when(regexp_extract(col("Crm Cd Desc"), violent_crimes, 0) != "", "Violent Crimes")
    .when(regexp_extract(col("Crm Cd Desc"), sex_crimes, 0) != "", "Sex Crimes")
    .when(regexp_extract(col("Crm Cd Desc"), property_crimes, 0) != "", "Property Crimes")
    .when(regexp_extract(col("Crm Cd Desc"), public_order_crimes, 0) != "", "Public Order & Weapon Crimes")
    .otherwise("Other Crimes")
)

def count_missing_values(df):
    print("Missing values in columns:")
    df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

def inspect_data(df):
    # Find min & max dates
    date_stats = df.select(
        min(col("Date Rptd")).alias("Earliest_Date_Rptd"),
        max(col("Date Rptd")).alias("Latest_Date_Rptd"),
        min(col("DATE OCC")).alias("Earliest_DATE_OCC"),
        max(col("DATE OCC")).alias("Latest_DATE_OCC")
    )

    # Get distinct crime codes
    distinct_crime_codes = df.select("Crm Cd").distinct().orderBy("Crm Cd")

    # Show results
    print("Date Range Stats:")
    date_stats.show()

    print("Distinct Crime Codes:")
    distinct_crime_codes.show(50, truncate=False)

    # Check duplicates
    dr_no_counts = df.groupBy("DR_NO").count().orderBy(col("count").desc())
    print("DR_NO Counts (Top 50):")
    dr_no_counts.show(50, truncate=False)

def filter_data(df):
    return (
        #Handle Missing Date
        df.withColumn(
            "DATE OCC",
            when(col("DATE OCC").isNull(), col("Date Rptd"))
            .when(col("DATE OCC").isNull() & col("Date Rptd").isNull(), to_timestamp(lit("2000-01-01"), "yyyy-MM-dd"))
            .otherwise(col("DATE OCC"))
        )
        .withColumn(
            "Date Rptd",
            when(col("Date Rptd").isNull(), col("DATE OCC"))
            .when(col("Date Rptd").isNull() & col("DATE OCC").isNull(), to_timestamp(lit("2000-01-01"), "yyyy-MM-dd"))
            .otherwise(col("Date Rptd"))
        )
        #Handle Missing Crime Cd and Desc
        .withColumn("Crm Cd", when(col("Crm Cd").isNull(), lit(9999)).otherwise(col("Crm Cd")))
        .withColumn("Crm Cd Desc", when(col("Crm Cd Desc").isNull(), lit("N/A")).otherwise(col("Crm Cd Desc")))

        #Handle invalid lat long
        .filter((col("LAT").between(-90, 90)) & (col("LON").between(-180, 180)))

        #Handle duplicates
        .dropDuplicates(["DR_NO"])
    )

#count_missing_values(crime_filtered_df)
#inspect_data(crime_filtered_df)
crime_filtered_df = filter_data(crime_filtered_df)
crime_filtered_df.write \
    .option("maxRecordsPerFile", 100000) \
    .mode("overwrite") \
    .parquet("../la_dataset/cleaned_LA_crimedata")




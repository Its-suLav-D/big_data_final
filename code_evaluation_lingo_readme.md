> CMD 5 

```python
poi_idaho = poi.filter(F.col("region") == "ID")

spatial = spatial.join(poi, on="placekey", how="left_semi")
spatial_idaho = spatial.filter(F.col("region") == "ID").join(poi_idaho, on="placekey", how="left_semi")
# 2019

pattern = pattern\
    .withColumn("year", F.year("date_range_start"))\
    .withColumn("month", F.month("date_range_start"))\
    .filter((F.col("year") == 2019))\
    .join(poi.select("placekey"), on = ['placekey'], how="inner")\
    .drop("year", "month", "visitor_country_of_origin")
pattern_idaho = pattern\
    .withColumn("year", F.year("date_range_start"))\
    .withColumn("month", F.month("date_range_start"))\
    .filter((F.col("year") == 2019))\
    .join(poi_idaho.select("placekey"), on = ['placekey'], how="inner")\
    .drop("year", "month", "visitor_country_of_origin")

tract_table = tract_table.filter(F.col("stusab") == "ID")

spark.sql("CREATE DATABASE chapel")
poi.write.saveAsTable("chapel.poi")
poi_idaho.write.saveAsTable("chapel.poi_idaho")
spatial_idaho.write.saveAsTable("chapel.spatial_idaho")
spatial.write.saveAsTable("chapel.spatial")
pattern.write.saveAsTable("chapel.pattern")
pattern_idaho.write.saveAsTable("chapel.pattern_idaho")
tract_table.write.saveAsTable("chapel.tract")
```

- In this code, we're dealing with a series of data manipulations using Apache Spark, focusing on Points of Interest (POI) in Idaho. Initially, the code filters the POI dataset to include only those from Idaho. It then creates two spatial data frames: the first includes all POI data, while the second is limited to Idaho. Both use a 'left semi-join', which means we're keeping only those records from the spatial data that have a matching entry in the POI data. The pattern data, which seems to represent visit patterns, is similarly filtered for the year 2019 and joined with the POI data. The process is repeated specifically for Idaho POI data. Additionally, there's a filter applied to a 'tract table' for Idaho. Finally, the code creates a new database and saves these various data frames as tables within it, effectively organizing our data around Idaho's POIs and spatial characteristics for easier access and analysis.

Improvement Suggestions:
- Replace hard-coded values like 'ID' and '2019' with variables for flexibility.



> CMD 22 
```python
windowPKD  = Window.partitionBy(["placekey", 'date_range_start']).orderBy(F.desc("value"))
pat = spark.table("chapel.pattern")
dayweek = pat.selectExpr("placekey", "normalized_visits_by_state_scaling", "raw_visitor_counts", "raw_visit_counts", "date_range_start", "placekey", "explode(popularity_by_day)")\
    .withColumn("rank", F.rank().over(windowPKD))\
    .groupBy(["placekey", "key"])\
    .agg(
        F.percentile_approx("rank", 0.7).alias("rank_70th"),
        F.percentile_approx("rank", 0.7).alias("rank_50th"))\
    .filter(
        ((F.col("rank_70th").isin([1,2,3])) & (F.col("key") == "Sunday")) |
        ((F.col("rank_70th").isin([7,6,5,4])) & (F.col("key") == "Monday")))\
    .groupBy("placekey")\
    .pivot("key")\
    .agg(F.first("rank_70th"))\
    .filter(F.col("Monday").isNotNull())\
    .filter(F.col("Sunday").isNotNull())\
    .join(poi.select("placekey", "street_address", "city", "region", "category_tags"), how="left",  on="placekey")
dayweek.write.saveAsTable("chapel.use_pattern_chapel")
display(dayweek)
```
This code block in PySpark is focused on analyzing visitation patterns at various Points of Interest (POIs). It begins by setting up a window function to partition data by placekey and date_range_start, ordering by the value field. The main operation involves loading visit pattern data (chapel.pattern), expanding it to assess daily popularity, and then calculating ranking percentiles for each placekey and day combination. The script specifically filters for significant rankings on Sundays and Mondays, likely to pinpoint popular visit days. After reshaping the data with a pivot operation and joining it with additional POI details (like address and city), the enriched dataset is saved as a new table for further use and displayed. This approach effectively identifies key visitation trends on specific days at different locations.

> CMD 50 
```python
nearest = spark.table("chapel.chapel_nearest")
pattern = spark.table("chapel.use_pattern_chapel")
final = pattern.join(
        nearest\
            .withColumnRenamed("street_address", "nearest_address")\
            .withColumnRenamed("initial_address", "scrape_address")\
            .select("placekey", "point", "point_chapel", "dist", "scrape_address", "nearest_address"),
        how="full", on="placekey")\
    .filter(
        (F.col("Sunday") == 1) |
        ((F.col("Sunday") == 2) & (F.col("dist").isNotNull())) |
        (F.col("dist") <= .0003) & (F.col("city").isNull()))\
    .sort("placekey", F.col("dist").desc())\
    .dropDuplicates(["placekey"])
display(final.limit(25))

```
In this PySpark code, two tables related to Points of Interest (POIs) are merged: chapel.chapel_nearest and chapel.use_pattern_chapel. The merge is executed with some column renaming for clarity, focusing on placekeys, distances, and addresses. Post-merge, the data is intricately filtered to focus on POIs with high visitation on Sundays or where the distance is within a specific threshold, indicating close proximity. This filtering likely aims to identify POIs with significant Sunday activity or those situated near certain addresses. After sorting by placekey and descending distance, duplicate entries are dropped, ensuring each placekey is uniquely represented. The final dataset, limited to the top 25 records, is then displayed, providing a focused view of key POIs based on the defined criteria.

Improvement Suggestion: The filter conditions are quite complex and could be broken down for better readability.

> CMD 60 
```python
pk_final = spark.table("chapel.placekey_chapel")
patterns = spark.table("chapel.pattern")
pk_tract = spark.table("safegraph.censustract_pkmap")

sunday = patterns.join(pk_final, how="inner", on="placekey")\
    .selectExpr("placekey", "posexplode_outer(visits_by_day)", "raw_visitor_counts",
                "raw_visit_counts","normalized_visits_by_state_scaling", "date_range_start", "street_address", "city", "region")\
    .join(pk_tract.select("placekey", "tractcode"), how="left", on="placekey")\
    .withColumn("date", F.date_add(F.col("date_range_start"), F.col("pos")))\
    .withColumn("dayofweek", F.dayofweek("date"))\
    .filter(F.col("dayofweek") == 1)\
    .groupBy(["placekey", "date_range_start", "raw_visit_counts", "normalized_visits_by_state_scaling"])\
    .agg(
        F.count("col").alias("count"),
        F.sum("col").alias("sum"),
        F.percentile_approx("col", .5).alias("median"),
        F.min("col").alias("min"), F.max("col").alias("max"))\
    .withColumn("sunday_visits", F.round((F.col("median") / F.col("raw_visit_counts")) * F.col("normalized_visits_by_state_scaling")))\
    .groupBy("placekey").agg(
    F.percentile_approx("sunday_visits", .5).alias("median"),
    F.max("sunday_visits").alias("max"),
    F.min("sunday_visits").alias("min"),
    F.count("sunday_visits").alias("count"))

# highest visitor count could be
sunday.write.saveAsTable("membership.sunday_visits")
display(sunday.limit(20))
```
- This PySpark code performs a detailed analysis of visit patterns at Points of Interest (POIs) specifically on Sundays. It begins by joining three tables - patterns of visits (chapel.pattern), placekeys specific to chapels (chapel.placekey_chapel), and a mapping of placekeys to census tracts (safegraph.censustract_pkmap). The join results in a selection of relevant columns including visit counts and location details. The code then calculates the specific date of visit and filters out data to keep only Sunday visits. Using this filtered data, it aggregates various statistics like count, sum, median, min, and max for visits. A new column sunday_visits is computed to represent normalized visit counts for Sundays. The final aggregation is done at the placekey level, providing a summary of Sunday visits per POI. The resulting table is then saved and displayed, showing the top 20 POIs with summarized Sunday visit statistics. This analysis is particularly useful for understanding POI popularity and activity levels on Sundays.



> CMD 64 
```python
window_unique = Window.partitionBy(["placekey"])
window_unique_month = Window.partitionBy(["placekey", "date_range_start"])
window_unique_home = Window.partitionBy(["home", "region"])

home_weight = spark.table("membership.home_dist")\
    .withColumn("raw_visitor_count_from_value", F.sum("value").over(window_unique_month))\
    .withColumn("total", F.col("dist") * F.col("value"))\
    .withColumn("sum", F.sum("total").over(window_pk))\
    .withColumn("n", F.sum("value").over(window_unique))\
    .withColumn("mean", F.col("sum") / F.col("n"))\
    .withColumn("std", F.stddev("dist").over(window_unique))\
    .withColumn("z-score", (F.col("dist") - F.col("mean")) / F.col("std"))\
    .filter(F.col("z-score") <= 2)\
    .withColumn("unique_pk_home", F.approx_count_distinct("placekey").over(window_unique_home))\
    .withColumn("unique_date_home", F.approx_count_distinct("date_range_start").over(window_unique_home))\
    .filter((F.col("unique_date_home") > 1) | (F.col("unique_pk_home") > 2))\
    .withColumn("raw_visitor_count_from_value_filtered", F.sum("value").over(window_unique_month))\
    .na.drop(subset=['value'])\
    .groupBy("placekey", "home")\
    .agg(
        F.sum("raw_visitor_count_from_value_filtered").alias("filtered_placekey"),
        F.sum("value").alias("filtered_home"),
        F.count("value").alias("count"))\
    .withColumn("home_weight", F.col("filtered_home") / F.col("filtered_placekey"))
home_weight.write.saveAsTable("membership.home_weight")

display(home_weight)

```

- This PySpark code is working on a dataset related to visitor home locations and their distances from various Points of Interest (POIs). It uses multiple window functions to compute statistics over different groupings of data. The code calculates the sum of visitor counts and distances within specified partitions, and derives a mean and standard deviation of distances for each POI. Notably, it employs a z-score calculation to identify and filter out outliers (distances that are more than two standard deviations from the mean). Further filtering is done to ensure meaningful aggregation, focusing on homes associated with more than one unique date or more than two unique POIs. The final aggregation step sums up the filtered visitor counts and computes a 'home weight' for each home-POI combination, representing the relative importance of each home location in the context of the POI. This resulting dataset, once saved as a table, provides a nuanced view of visitor distribution and can be a valuable asset for spatial analysis or location-based strategic planning.



> CMD 66 
```python
dat = home_weight.select("placekey", "home", "filtered_home", "filtered_placekey", "home_weight")\
    .join(sunday, how="left", on="placekey")\
    .withColumn("typical_sunday", F.when(F.col("median") <= 16, F.col("max")).otherwise(F.col("median")))\
    .withColumn("active_members", F.col("median") * F.col("home_weight"))\
    .groupBy("home")\
    .agg(F.round(F.sum("active_members"), 0).alias("active_members_estimate"))
dat.write.saveAsTable("membership.tract_membership")
display(dat)
```
- the PySpark code is combining data from two different sources to estimate active membership by home locations. It starts with the home_weight table, selecting key columns including the calculated 'home_weight' for each placekey-home pair. This table is then joined with the sunday table to integrate data about typical visit patterns on Sundays. A new column, 'typical_sunday', is calculated to determine the usual number of visitors on Sundays - using the maximum value if the median is less than or equal to 16, or the median otherwise. Next, an 'active_members' column is created by multiplying this typical Sunday visit count with the home weight. The data is then grouped by home location, and the sum of active members is aggregated and rounded to provide an estimate of active membership per home location. This resultant dataset, a comprehensive view of estimated active membership based on visit patterns and home locations, is saved as a new table and displayed. This analysis is especially useful in understanding the distribution of active members across different home locations, providing valuable insights for community engagement or resource allocation strategies.

> CMD 74 
```python
final = county_target\
    .join(
        rel_cens.withColumnRenamed("Church of Jesus Christ of Latter-day Saints", "rcensus_lds")\
            .select("STATEFP", "COUNTYFP", "State Name", "County Name", "rcensus_lds"),
    how="left", on = ["STATEFP", "COUNTYFP"])\
    .withColumn("ratio_census", F.round(F.col("active_members_estimate") / F.col("rcensus_lds"), 2))\
    .withColumn("ratio_population", F.round(F.col("active_members_estimate") / F.col("population"), 2))\
    .withColumnRenamed("State Name", "state_name")\
    .withColumnRenamed("County Name", "county_name")
final.write.saveAsTable("membership.county_active_populations_ldscensus")
display(final)
```
- This code merges two datasets to analyze the distribution of a religious community's active membership across various counties. It starts by joining the county_target DataFrame with the rel_cens DataFrame, which contains religious census data. In rel_cens, the column for the Church of Jesus Christ of Latter-day Saints is renamed to rcensus_lds for clarity. The join is based on state and county FIPS codes (STATEFP, COUNTYFP). After this join, two ratios are calculated: one (ratio_census) representing the ratio of estimated active members to the LDS census count, and another (ratio_population) representing the ratio of active members to the total population of the county. These ratios provide a clear comparative view of the active membership against both the specific religious census and the broader county population. Finally, state and county names are standardized in column naming, and the resulting DataFrame is saved as a table and displayed. This process offers a detailed and comparative insight into the active membership's distribution and density, which can be vital for community planning or understanding religious demographics.

# Vocabulary/Lingo Challenge 

1. Using Databricks in Data Science:
- As a student learning data science, I find Databricks quite a game-changer. It's like having a powerful ally that handles the heavy lifting of big data processing. The ease of collaborating on shared notebooks and the seamless integration with various data science tools make it super convenient, especially for group projects. It's almost like Databricks does the tough part, allowing me to focus on the actual data analysis and learning.

2. PySpark vs. Pandas/Tidyverse:
Comparing PySpark with Pandas or Tidyverse is like choosing between a cargo truck and a sports car. PySpark, with its capacity for handling massive datasets distributed across clusters, is like the cargo truck - essential for heavy loads but not as nimble for quick tasks. On the other hand, Pandas or Tidyverse, perfect for data munging in smaller datasets, are like sports cars - fast, efficient, and easy to maneuver for day-to-day data tasks. As a student, I lean towards Pandas for most classwork due to its simplicity, but I know PySpark is crucial for the big data challenges in the real world.

3. Explaining Docker:
To explain Docker to someone not tech-savvy, I’d compare it to a portable kitchen. Just as a portable kitchen has everything you need to cook a meal anywhere, Docker packages software with all its necessary components, so it runs the same no matter where you use it. This consistency is a lifesaver in software development - it’s like ensuring your favorite recipe tastes the same whether you cook it at home or in a friend's kitchen. For developers, Docker means less time worrying about setup and more time creating amazing software.






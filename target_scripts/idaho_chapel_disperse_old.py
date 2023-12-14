# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import plotly.express as px

# COMMAND ----------

spark.sql("SHOW TABLES in safegraph").toPandas()

# COMMAND ----------

poi = spark.table("safegraph.places").filter(
    (F.col("top_category") == "Religious Organizations") &
    (F.col("location_name").rlike("Latter|latter|Saints|saints|LDS|\b[Ww]ard\b")) &
    (F.col("location_name").rlike("^((?!Reorganized).)*$")) &
    (F.col("location_name").rlike("^((?!All Saints).)*$")) &
    (F.col("location_name").rlike("^((?![cC]ath).)*$")) &
    (F.col("location_name").rlike("^((?![Bb]ody).)*$")) &
    (F.col("location_name").rlike("^((?![Pp]eter).)*$")) &
    (F.col("location_name").rlike("^((?![Cc]atholic).)*$")) &
    (F.col("location_name").rlike("^((?![Pp]res).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]inist).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]ission).)*$")) &
    (F.col("location_name").rlike("^((?![Ww]orship).)*$")) &
    (F.col("location_name").rlike("^((?![Rr]ain).)*$")) &
    (F.col("location_name").rlike("^((?![Bb]aptist).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]eth).)*$")) &
    (F.col("location_name").rlike("^((?![Ee]vang).)*$")) &
    (F.col("location_name").rlike("^((?![Ll]utheran).)*$")) &
    (F.col("location_name").rlike("^((?![Oo]rthodox).)*$")) &
    (F.col("location_name").rlike("^((?![Ee]piscopal).)*$")) &
    (F.col("location_name").rlike("^((?![Tt]abernacle).)*$")) &
    (F.col("location_name").rlike("^((?![Hh]arvest).)*$")) &
    (F.col("location_name").rlike("^((?![Aa]ssem).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]edia).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]artha).)*$")) &
    (F.col("location_name").rlike("^((?![Cc]hristian).)*$")) &
    (F.col("location_name").rlike("^((?![Uu]nited).)*$")) &
    (F.col("location_name").rlike("^((?![Ff]ellowship).)*$")) &
    (F.col("location_name").rlike("^((?![Ww]esl).)*$")) &
    (F.col("location_name").rlike("^((?![C]cosmas).)*$")) &
    (F.col("location_name").rlike("^((?![Gg]reater).)*$")) &
    (F.col("location_name").rlike("^((?![Pp]rison).)*$")) &
    (F.col("location_name").rlike("^((?![Cc]ommuni).)*$")) &
    (F.col("location_name").rlike("^((?![Cc]lement).)*$")) &
    (F.col("location_name").rlike("^((?![Vv]iridian).)*$")) &
    (F.col("location_name").rlike("^((?![Dd]iocese).)*$")) &
    (F.col("location_name").rlike("^((?![Hh]istory).)*$")) &
    (F.col("location_name").rlike("^((?![Ss]chool).)*$")) &
    (F.col("location_name").rlike("^((?![Tt]hougt).)*$")) &
    (F.col("location_name").rlike("^((?![Hh]oliness).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]artyr).)*$")) &
    (F.col("location_name").rlike("^((?![Jj]ames).)*$")) &
    (F.col("location_name").rlike("^((?![Ff]ellowship).)*$")) &
    (F.col("location_name").rlike("^((?![Hh]ouse).)*$")) &
    (F.col("location_name").rlike("^((?![Gg]lory).)*$")) &
    (F.col("location_name").rlike("^((?![Aa]nglican).)*$")) &
    (F.col("location_name").rlike("^((?![Pp]oetic).)*$")) &
    (F.col("location_name").rlike("^((?![Ss]anctuary).)*$")) &
    (F.col("location_name").rlike("^((?![Ee]quipping).)*$")) &
    (F.col("location_name").rlike("^((?![Jj]ohn).)*$")) &
    (F.col("location_name").rlike("^((?![Aa]ndrew).)*$")) &
    (F.col("location_name").rlike("^((?![Ee]manuel).)*$")) &
    (F.col("location_name").rlike("^((?![Rr]edeemed).)*$")) &
    (F.col("location_name").rlike("^((?![Pp]erfecting).)*$")) &
    (F.col("location_name").rlike("^((?![Aa]ngel).)*$")) &
    (F.col("location_name").rlike("^((?![Aa]rchangel).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]icheal).)*$")) &
    (F.col("location_name").rlike("^((?![Tt]hought).)*$")) &
    (F.col("location_name").rlike("^((?![Pp]ariosse).)*$")) &
    (F.col("location_name").rlike("^((?![Cc]osmas).)*$")) &
    (F.col("location_name").rlike("^((?![Dd]eliverance).)*$")) &
    (F.col("location_name").rlike("^((?![Ss]ociete).)*$")) &
    (F.col("location_name").rlike("^((?![Tt]emple).)*$")) &
    (F.col("location_name").rlike("^((?![Ss]eminary).)*$")) &
    (F.col("location_name").rlike("^((?![Ee]mployment).)*$")) &
    (F.col("location_name").rlike("^((?![Ii]nstitute).)*$")) &
    (F.col("location_name").rlike("^((?![Cc]amp).)*$")) &
    (F.col("location_name").rlike("^((?![Ss]tudent).)*$")) &
    (F.col("location_name").rlike("^((?![Ee]ducation).)*$")) &
    (F.col("location_name").rlike("^((?![Ss]ocial).)*$")) &
    (F.col("location_name").rlike("^((?![Ww]welfare).)*$")) &
    (F.col("location_name").rlike("^((?![Cc][Ee][Ss]).)*$")) &
    (F.col("location_name").rlike("^((?![Ff]amily).)*$")) &
    (F.col("location_name").rlike("^((?![Mm]ary).)*$")) &
    (F.col("location_name").rlike("^((?![Rr]ussian).)*$")) &
    (F.col("location_name").rlike("^((?![Bb]eautif).)*$")) &
    (F.col("location_name").rlike("^((?![Hh]eaven).)*$")) &    
    (F.col("location_name").rlike("^((?!Inc).)*$")) &
    (F.col("location_name").rlike("^((?!God).)*$")))\
    .dropDuplicates(["placekey"])
pattern = spark.table("safegraph.patterns").dropDuplicates(['placekey', 'date_range_start'])
tract_table = spark.table("safegraph.tract_table")
spatial = spark.table("safegraph.spatial").join(poi, how="left_semi", on="placekey")

# COMMAND ----------

display(poi)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filter data

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Temple and Chapel Scrape Support

# COMMAND ----------

chapel = spark.read.parquet("dbfs:/mnt/azurestorage/chapel/full_church_building_data-20.parquet")
display(chapel.limit(5))

# COMMAND ----------

temple = spark.read.parquet("dbfs:/mnt/azurestorage/chapel/temple_details_spatial.parquet")
display(temple.limit(5))

# COMMAND ----------

tract_temple_distance = spark.read.parquet("dbfs:/mnt/azurestorage/chapel/tract_distance_to_nearest_temple.parquet")
display(tract_temple_distance.limit(5))

# COMMAND ----------

chapel.write.saveAsTable("chapel.chapel_scrape")
temple.write.saveAsTable("chapel.temple_scrape")
tract_temple_distance.write.saveAsTable("chapel.tract_temple_distance")

# COMMAND ----------

spark.sql("SHOW TABLES in chapel").toPandas()

# COMMAND ----------

poi = spark.table("chapel.poi_idaho")
display(poi)

# COMMAND ----------

spatial = spark.table("chapel.spatial_idaho")
display(spatial)

# COMMAND ----------

patterns = spark.table("chapel.pattern")
display(patterns)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Alogrithm
# MAGIC
# MAGIC ### With spatial, patterns, and poi
# MAGIC
# MAGIC 1. Filter to the chapels   
# MAGIC   A. Check that the location has an appropriate name with Regex (see above)   
# MAGIC   B. Just use buildings in the religious subcategory (see above)   
# MAGIC   C. Validate Sunday, Monday patterns rule     
# MAGIC   D. Leverage Copeland's scrape to find the nearest placekey to each scraped lat long   
# MAGIC
# MAGIC ## With patterns
# MAGIC
# MAGIC 2. Use the `visitor_home_aggregation` column on the chapels rows to create a table with `normalized_visits_by_state_scaling`, `raw_visitor_counts`, `raw_visit_counts`, `date_range_start`, `placekey` and `key`, `value` from the first column.

# COMMAND ----------

chapel_scrape = spark.table("chapel.chapel_scrape").groupBy("state").count()
display(chapel_scrape)

# COMMAND ----------

#spark.sql("DROP TABLE use_pattern_chapel_idaho")
#spark.sql("DROP table chapel_nearest_idaho")

# COMMAND ----------

windowPKD  = Window.partitionBy(["placekey", 'date_range_start']).orderBy(F.desc("value"))
patid = spark.table("chapel.pattern_idaho")
dayweek = patid.selectExpr("placekey", "normalized_visits_by_state_scaling", "raw_visitor_counts", "raw_visit_counts", "date_range_start", "placekey", "explode(popularity_by_day)")\
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
dayweek.write.saveAsTable("chapel.use_pattern_chapel_idaho")
display(dayweek)
# 204 locations

# COMMAND ----------

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


# COMMAND ----------

# We appear to be missing half of the churches
spark.table("chapel.chapel_scrape").count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Find nearest locations

# COMMAND ----------

spark.sql("USE chapel")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sp_pk_id as 
# MAGIC SELECT ST_Point(spatial_idaho.longitude, spatial_idaho.latitude) as point, placekey, street_address, city
# MAGIC FROM spatial_idaho

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sp_pk as 
# MAGIC SELECT ST_Point(spatial.longitude, spatial.latitude) as point, placekey, street_address, city
# MAGIC FROM spatial

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sp_chapel as 
# MAGIC SELECT ST_Point(lon, lat) as point_chapel,initial_address
# MAGIC FROM chapel_scrape

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sp_chapel_id as 
# MAGIC SELECT ST_Point(lon, lat) as point_chapel,initial_address
# MAGIC FROM chapel_scrape
# MAGIC WHERE state = "id"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE chapel_nearest_idaho as 
# MAGIC SELECT point, point_chapel, placekey, street_address, initial_address, ST_DISTANCE(sp_chapel_id.point_chapel, sp_pk_id.point) as dist
# MAGIC FROM  sp_pk_id, sp_chapel_id
# MAGIC WHERE ST_DISTANCE(sp_chapel_id.point_chapel, sp_pk_id.point) < .006

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE chapel_nearest as 
# MAGIC SELECT point, point_chapel, placekey, street_address, initial_address, ST_DISTANCE(sp_chapel.point_chapel, sp_pk.point) as dist
# MAGIC FROM  sp_pk, sp_chapel
# MAGIC WHERE ST_DISTANCE(sp_chapel.point_chapel, sp_pk.point) < .006

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now join the spatial close ones and the pattern ones

# COMMAND ----------

nearest = spark.table("chapel.chapel_nearest_idaho")
pattern = spark.table("chapel.use_pattern_chapel_idaho")
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
display(final)
# 218 placekeys that match our spatial and temporal criteria

# COMMAND ----------

spark.table("chapel.sp_pk_id").join(final, how="leftsemi", on="placekey").createOrReplaceTempView("keep_keys")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE temp_dist as
# MAGIC SELECT point, point_chapel, placekey, street_address, initial_address, ST_DISTANCE(sp_chapel_id.point_chapel, keep_keys.point) as dist
# MAGIC FROM  keep_keys, sp_chapel_id
# MAGIC ORDER BY dist DESC

# COMMAND ----------

temp = spark.table("chapel.temp_dist").sort(F.col("dist").asc()).dropDuplicates(["placekey"])
poi_idaho = spark.table("chapel.poi_idaho")
temp.join(poi_idaho.select("poi_cbg", "location_name", "street_address", "city", "region", "postal_code", "wkt_area_sq_meters", "placekey"), how="left", on=["placekey", "street_address"])\
    .select("placekey", "street_address", "city", "region", "postal_code", "wkt_area_sq_meters", "initial_address", "dist", "point", "point_chapel", "location_name")\
    .write.saveAsTable("chapel.placekey_chapel_idaho")
spark.sql("DROP TABLE temp_dist")


# COMMAND ----------

pk_final_idaho = spark.table("chapel.placekey_chapel_idaho")
patterns_idaho = spark.table("chapel.pattern_idaho")
pk_tract = spark.table("safegraph.censustract_pkmap")

window_unique = Window.partitionBy(["placekey", 'date_range_start'])
window_pk = Window.partitionBy(["placekey"]).orderBy("raw_visitor_counts")

dat = patterns_idaho.join(pk_final_idaho, how="inner", on="placekey")\
    .selectExpr("placekey", "explode_outer(visitor_home_aggregation)", "raw_visitor_counts",
                "raw_visit_counts","normalized_visits_by_state_scaling", "date_range_start", "street_address", "city", "region")\
    .join(pk_tract.select("placekey", "tractcode"), how="left", on="placekey")\
    .withColumns({
        "home": F.when(F.col("key").isNull(), F.col("tractcode")).otherwise(F.col("key")),
        "people": F.when(F.col("value").isNull(), F.col("raw_visitor_counts")).otherwise(F.col("value"))})\
    .withColumn("total_tract", F.sum("people").over(window_unique))\
    .withColumn("month_rank", F.dense_rank().over(window_pk))\
    .filter(F.col("month_rank").isin([3,4,5,6,7,8,9,10]))\
    .withColumn("tract_scale", F.col("raw_visitor_counts") / F.col("total_tract"))\
    .withColumn("tract_visitor", F.col("tract_scale") * F.col("value") * 16)\
    .select("home", "tract_visitor", "raw_visitor_counts", "raw_visit_counts", "people", "total_tract", "tract_scale", "date_range_start", "placekey")\
    .groupBy("home", "placekey")\
    .agg(
        F.min("tract_visitor").alias("min_visitor"),
        F.percentile_approx("tract_visitor", .5).alias("median_visitor"),
        F.max("tract_visitor").alias("max_visitor"),
        F.count("tract_visitor").alias("count"))\
    .groupBy("home")\
    .agg(
        F.sum("min_visitor").alias("min_visitor"),
        F.sum("median_visitor").alias("median_visitor"),
        F.sum("max_visitor").alias("max_visitor"),
        F.count("placekey").alias("count"))


# COMMAND ----------

# If we don't have any members in a visitor_home_aggregation we will put them all in the building tract.
# We have 12 months. We know that April and October will have problems because of Conference.  We worry about Stake Conference making some chapels have massive monthly numbers.
# Trim the two worst months and two best months to use 8 months for member dispersement.
# if the visitor_home_aggregation is more than raw_visitor_count we scale them all down to match "{""16059970100"":5,""16083000600"":4,""16083000500"":4}"	245.4888931813792 10 # notice 13 in tract bo 10 total
# if the visitor_home_aggregation is less than raw_visit_count we scale them all up to match "{""16029960100"":8,""16005000300"":4}"	245.75100887812752	15 notice 12 in tract but 15 total
# rule: A chapel can't have less than 30 people attending it....
dat.write.saveAsTable("chapel.tract_idaho")
display(dat)
# 1 visit is equavalent to 16 scaling
# https://datacommons.org/place/geoId/16065950302?category=Health

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Now run for the Entire US

# COMMAND ----------

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
display(final)
# 3,213 placekeys that match our spatial and temporal criteria

# COMMAND ----------

spark.table("chapel.sp_pk").join(final, how="leftsemi", on="placekey").createOrReplaceTempView("keep_keys")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECt * FROM keep_keys

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE temp_dist as
# MAGIC SELECT point, point_chapel, placekey, street_address, initial_address, ST_DISTANCE(sp_chapel.point_chapel, keep_keys.point) as dist
# MAGIC FROM  keep_keys, sp_chapel
# MAGIC ORDER BY dist DESC

# COMMAND ----------

temp = spark.table("chapel.temp_dist").sort(F.col("dist").asc()).dropDuplicates(["placekey"])
poi = spark.table("chapel.poi")
temp.join(poi.select("poi_cbg", "location_name", "street_address", "city", "region", "postal_code", "wkt_area_sq_meters", "placekey"), how="left", on=["placekey", "street_address"])\
    .select("placekey", "street_address", "city", "region", "postal_code", "wkt_area_sq_meters", "initial_address", "dist", "point", "point_chapel", "location_name")\
    .write.saveAsTable("chapel.placekey_chapel")
spark.sql("DROP TABLE temp_dist")

# COMMAND ----------

dat = spark.table("chapel.placekey_chapel")
display(dat)

# COMMAND ----------

# We appear to be missing 1/4 of the churches
spark.table("chapel.chapel_scrape").count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Now spread members to tracts

# COMMAND ----------

pk_final = spark.table("chapel.placekey_chapel")
patterns = spark.table("chapel.pattern")
pk_tract = spark.table("safegraph.censustract_pkmap")

window_unique = Window.partitionBy(["placekey", 'date_range_start'])
window_pk = Window.partitionBy(["placekey"]).orderBy("raw_visitor_counts")

dat = patterns.join(pk_final, how="inner", on="placekey")\
    .selectExpr("placekey", "explode_outer(visitor_home_aggregation)", "raw_visitor_counts",
                "raw_visit_counts","normalized_visits_by_state_scaling", "date_range_start", "street_address", "city", "region")\
    .join(pk_tract.select("placekey", "tractcode"), how="left", on="placekey")\
    .withColumns({
        "home": F.when(F.col("key").isNull(), F.col("tractcode")).otherwise(F.col("key")),
        "people": F.when(F.col("value").isNull(), F.col("raw_visitor_counts")).otherwise(F.col("value"))})\
    .withColumn("total_tract", F.sum("people").over(window_unique))\
    .withColumn("month_rank", F.dense_rank().over(window_pk))\
    .filter(F.col("month_rank").isin([3,4,5,6,7,8,9,10]))\
    .withColumn("tract_scale", F.col("raw_visitor_counts") / F.col("total_tract"))\
    .withColumn("tract_visitor", F.col("tract_scale") * F.col("value") * 16)\
    .select("home", "tract_visitor", "raw_visitor_counts", "raw_visit_counts", "people", "total_tract", "tract_scale", "date_range_start", "placekey")\
    .groupBy("home", "placekey")\
    .agg(
        F.min("tract_visitor").alias("min_visitor"),
        F.percentile_approx("tract_visitor", .5).alias("median_visitor"),
        F.max("tract_visitor").alias("max_visitor"),
        F.count("tract_visitor").alias("count"))\
    .groupBy("home")\
    .agg(
        F.sum("min_visitor").alias("min_visitor"),
        F.sum("median_visitor").alias("median_visitor"),
        F.sum("max_visitor").alias("max_visitor"),
        F.count("placekey").alias("count"))\
    .filter(F.col("min_visitor").isNotNull())

# COMMAND ----------

dat.write.saveAsTable("chapel.tract_usa")
display(dat)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Now we need to check our work and adjust
# MAGIC
# MAGIC

# COMMAND ----------

dat = spark.table("chapel.tract_usa")
tractp = spark.table("census.tractpopulations").withColumnRenamed("tractcode", "home")
# tract_sp = spark.table("census.tract_spatial").withColumnRenamed("GEOID", "home") # broken only has 206 rows. Need to fix
compare = tractp.join(dat, how="full", on="home").fillna(0).withColumns({
    "proportion_min":F.col("min_visitor") / F.col("population"),
    "proportion_median":F.col("median_visitor") / F.col("population"),
    "proportion_max":F.col("max_visitor") / F.col("population")
    })#.join(tract_sp, how="full", on="home")
compare.write.saveAsTable("chapel.tract_usa_populations")
display(compare)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Let's aggregate by county and then use the Religous census as a check
# MAGIC - Assumption: The religion census should be a larger number than our calculation
# MAGIC - Assumption: The religion census should be smaller than the county total
# MAGIC - Check Copeland's scrape data for unique addresses
# MAGIC

# COMMAND ----------

tract_target = spark.table("chapel.tract_usa_populations")
county_tract_map = spark.table("census.tract_spatial").withColumnRenamed("GEOID","home").select("home", "STATEFP", "COUNTYFP")
county_target = tract_target.select("home", "population", "min_visitor", "median_visitor", "max_visitor")\
    .join(county_tract_map, on="home", how="left")\
    .groupBy(["STATEFP", "COUNTYFP"])\
    .agg(
        F.sum("population").alias("population"),
        F.sum("min_visitor").alias("min_visitor"),
        F.sum("median_visitor").alias("median_visitor"),
        F.sum("max_visitor").alias("max_visitor"))
display(county_target)

# COMMAND ----------

# https://www.usreligioncensus.org/part#:~:text=Data%20sources%3A%20Our%20data%20collectors,their%20own%20statistics%20to%20us.
# https://www.usreligioncensus.org/sites/default/files/2023-11/2020Participants_previous_participation.pdf

rel_cens = spark.read.parquet("dbfs:/mnt/azurestorage/county_data/religion_census")
display(rel_cens)

# COMMAND ----------

final = county_target\
    .withColumns({
        "min_visitor": F.round("min_visitor", 0),
        "median_visitor": F.round("median_visitor", 0),
        "max_visitor": F.round("max_visitor", 0)})\
    .join(
        rel_cens.withColumnRenamed("Church of Jesus Christ of Latter-day Saints", "rcensus_lds")\
            .select("STATEFP", "COUNTYFP", "State Name", "County Name", "rcensus_lds"),
    how="left", on = ["STATEFP", "COUNTYFP"])\
    .withColumn("ratio_census", F.round(F.col("median_visitor") / F.col("rcensus_lds"), 2))\
    .withColumn("ratio_population", F.round(F.col("median_visitor") / F.col("population"), 2))\
    .withColumnRenamed("State Name", "state_name")\
    .withColumnRenamed("County Name", "county_name")
final.write.saveAsTable("chapel.tract_usa_populations_ldscensus")
display(final)

# COMMAND ----------

df = spark.table("chapel.tract_usa_populations_ldscensus").toPandas()

# COMMAND ----------

px.histogram(df, x="ratio_census")

# COMMAND ----------

px.histogram(df, x="ratio_population")

# COMMAND ----------

px.scatter(df, x="median_visitor", y="rcensus_lds", color="population")

# COMMAND ----------



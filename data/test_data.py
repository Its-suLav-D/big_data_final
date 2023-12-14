#%%
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
# %%
county = pl.read_parquet('../data/active_members_county.parquet')
tract = pl.read_parquet("../data/active_members_tract.parquet")
chapel_scrape = pl.read_parquet("../data/full_church_building_data-20.parquet")
chapel_safegraph = pl.read_parquet("../data/safegraph_chapel.parquet")
temples = pl.from_arrow(pq.read_table("../data/temple_details_spatial.parquet"))
tract_nearest = pl.from_arrow(pq.read_table("../data/tract_distance_to_nearest_temple.parquet"))
# %%



 
import streamlit as st 
import polars as pl 
import pyarrow.parquet as pq 
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import scipy.stats as stats
import json
import pydeck as pdk
from polars import col
import pandas as pd 


def load_geo_data():
    with open('./data/georef-united-states-of-america-county.geojson') as f:
        data = json.load(f)
    return data

def load_data():
    tract = pl.read_parquet("./data/active_members_tract.parquet")
    county = pl.read_parquet("./data/active_members_county.parquet")
    temples = pl.from_arrow(pq.read_table("./data/temple_details_spatial.parquet"))
    tract_nearest = pl.from_arrow(pq.read_table("./data/tract_distance_to_nearest_temple.parquet"))
    return tract, county, tract_nearest, temples

def filter_geo_data(geo_data, county_fp, state_fp):
    filtered_features = [feature for feature in geo_data['features'] if
                         feature['properties']['coty_fp_code'] == county_fp and
                         feature['properties']['ste_code'][0] == state_fp]
    return filtered_features

def create_random_data_frame(center_lat, center_lon, num_points, radius=0.01):
    """
    Create a Pandas DataFrame with random points around a center latitude and longitude.
    """
    lats = np.random.normal(center_lat, radius, num_points)
    lons = np.random.normal(center_lon, radius, num_points)

    return pd.DataFrame({'lat': lats, 'lon': lons})

def create_map(filtered_feature, active_members_estimate, census_per_capita_estimate):
    geometry = filtered_feature[0]['geometry']
    center_lat = float(filtered_feature[0]['properties']['geo_point_2d']['lat'])
    center_lon = float(filtered_feature[0]['properties']['geo_point_2d']['lon'])

    # Create random points for active members estimate
    random_points_df = create_random_data_frame(center_lat, center_lon, int(active_members_estimate), radius=0.01)



    geojson_layer = pdk.Layer(
        "GeoJsonLayer",
        data={'type': 'FeatureCollection', 'features': [filtered_feature[0]]},
        get_fill_color=[0, 0, 255, 80],
        get_line_color=[255, 255, 255],
        line_width_min_pixels=2,
    )

    scatterplot_layer = pdk.Layer(
        "ScatterplotLayer",
        data=random_points_df,
        get_position='[lon, lat]',
        get_color=[200, 30, 0, 160],
        get_radius=200, 
    )


    # Create the Deck with both layers
    view_state = pdk.ViewState(latitude=center_lat,
                                           map_style='mapbox://styles/mapbox/light-v9',
 longitude=center_lon, zoom=9, pitch=50)
    map_deck = pdk.Deck(
        layers=[geojson_layer, scatterplot_layer],
        initial_view_state=view_state,
    )

    return map_deck

def display_state_selector(county_df):
    states = county_df['state_name'].unique().to_pandas().sort_values()
    state = st.sidebar.selectbox("Select a State:", [''] + states.tolist())
    return state

def display_county_selector(county_df, state):
    if state:
        counties = county_df.filter(col("state_name") == state)['county_name'].unique().to_pandas().sort_values()
        county = st.sidebar.selectbox("Select a County:", [''] + counties.tolist())
        return county
    return ''

def advanced_data_analysis(county_df):
    # Calculate additional metrics
    county_df = county_df.with_columns([
        (county_df['active_members_estimate'] - county_df['rcensus_lds']).alias('difference'),
        (county_df['active_members_estimate'] / county_df['rcensus_lds']).alias('ratio'),
        (county_df['active_members_estimate'] / county_df['population']).alias('members_per_capita'),
        (county_df['rcensus_lds'] / county_df['population']).alias('census_per_capita')
    ])

    # Perform correlation analysis
    correlation_matrix = county_df[['active_members_estimate', 'rcensus_lds', 'population', 'difference', 'ratio', 'members_per_capita', 'census_per_capita']].corr()

    return county_df, correlation_matrix



def visualize_correlation_matrix(correlation_matrix):
    fig = px.imshow(correlation_matrix.to_pandas(), 
                    labels=dict(color="Correlation"),
                    x=correlation_matrix.columns,
                    y=correlation_matrix.columns,
                    title="Correlation Matrix")
    return fig

def visualize_scatter_matrix(county_df):
    fig = px.scatter_matrix(county_df.to_pandas(), 
                            dimensions=['active_members_estimate', 'rcensus_lds', 'population', 'difference', 'ratio'],
                            title="Scatter Matrix of Key Metrics")

    fig.update_yaxes(
        scaleanchor = "x",
        scaleratio = 1,
    )

    fig.update_layout(height=1200)  

    return fig


def main():
    st.title("County Data Visualization")
    geo_data = load_geo_data()
    tract, county, tract_nearest, temples = load_data()

    # Session state to hold selections
    if 'selected_state' not in st.session_state:
        st.session_state['selected_state'] = ''
    if 'selected_county' not in st.session_state:
        st.session_state['selected_county'] = ''

    # State selector
    st.session_state['selected_state'] = display_state_selector(county)

    # County selector, only show if a state has been selected
    st.session_state['selected_county'] = display_county_selector(county, st.session_state['selected_state'])


    st.subheader("Data Sample")
    st.dataframe(county.head())

    analyzed_data, correlation_matrix = advanced_data_analysis(county)
    corr_fig = visualize_correlation_matrix(correlation_matrix)
    scatter_matrix_fig = visualize_scatter_matrix(analyzed_data)

    st.plotly_chart(corr_fig)
    st.plotly_chart(scatter_matrix_fig)



    # Visualization button
    if st.sidebar.button('Run Spatial Analysis') and st.session_state['selected_state'] and st.session_state['selected_county']:
        st.sidebar.write("Scroll down to view the map visualization.")
        selected_county_fp = county.filter(col("county_name") == st.session_state['selected_county'])["COUNTYFP"].to_list()[0]
        selected_state_fp = county.filter(col("state_name") == st.session_state['selected_state'])["STATEFP"].to_list()[0]

        # Retrieve the active members estimate for the selected county
        active_members_estimate = analyzed_data.filter(
            (col("COUNTYFP") == selected_county_fp) & (col("STATEFP") == selected_state_fp)
        )["active_members_estimate"].to_numpy()[0]
        census_per_capita_estimate = analyzed_data.filter(
            (col("COUNTYFP") == selected_county_fp) & (col("STATEFP") == selected_state_fp)
        )["census_per_capita"].to_numpy()[0]

        # Custom legend using Markdown
        st.markdown(f"""
        **Map Legend**
        - 🟥 Red : Active Members : {active_members_estimate}
        - 🔵 Census Per Capita Estimate: {census_per_capita_estimate}
        """)

        filtered_feature = filter_geo_data(geo_data, selected_county_fp, selected_state_fp)
        if filtered_feature:
            map_deck = create_map(filtered_feature, active_members_estimate, census_per_capita_estimate)
            st.pydeck_chart(map_deck)


        else:
            st.error("No data found for the selected county.")


    st.subheader("Insights and Interpretation")
    st.write("""
    The analysis involving active member estimates and religious census estimates by county provides a multifaceted view of the religious landscape at the county level:

    - **Correlation Analysis:** The correlation matrix highlights the relationships between different metrics such as active members, religious census data, and population. A strong correlation between active member estimates and census data would suggest that the estimates are in line with expected patterns based on known census figures.

    - **Scatter Matrix Exploration:** The scatter matrix visualizes relationships among key metrics. This is crucial for identifying trends, outliers, or anomalies in the data. For instance:
        - A linear relationship between active members and census data would indicate consistency and reasonableness in the estimates.
        - Outliers or significant deviations might hint at discrepancies or unique situations in certain counties that warrant further investigation.

    - **Spatial Analysis:** The map visualization, combining geographical data with active member and census estimates, provides a spatial dimension to the analysis. It helps in understanding how the distribution of active members corresponds to the census data across different regions.
        - Clusters of high active membership in areas with lower census estimates could indicate regions with particularly strong religious engagement or reporting.
        - Conversely, areas with high census figures but lower active membership estimates might reflect regions with less engagement or different religious demographics.

    - **Comparative Analysis:** The combination of different data types (active membership estimates and census data) and the use of various analytical and visualization tools (correlation matrix, scatter matrix, spatial analysis) enrich the understanding of how reasonable the active member estimates are. It also sheds light on the demographic and geographic diversity within the religious landscape.

    In conclusion, the reasonableness of active member estimates in comparison to religious census estimates is not just a matter of numerical comparison but also involves understanding the geographical, demographic, and social contexts reflected in these figures. The provided visualizations and analyses contribute to a comprehensive assessment of this reasonableness, highlighting areas of congruence and divergence between the two data sets.
    """)



if __name__ == "__main__":
    main()



import streamlit as st
import polars as pl
import pyarrow.parquet as pq
import plotly.express as px
import json 
import pydeck as pdk
from polars import col
import pandas as pd 

class Analysis:
    def __init__(self, geo_json_file):
        self.geo_json_file = geo_json_file
        self.df_county = None
        self.df_temples = None 
        self.geo_json = None
        self.selected_state = None
        self.selected_county = None
    
    def load_geo_json_data(self):
        with open(self.geo_json_file) as f:
            self.geo_json = json.load(f)

    def load_data(self):
        tract, county, tract_nearest, temples = self._load_parquet_data()
        self.df_county = county
        self.df_temples = temples.filter(col('country') == 'United States')

    def _load_parquet_data(self):
        tract = pl.read_parquet("./data/active_members_tract.parquet")
        county = pl.read_parquet("./data/active_members_county.parquet")
        temples = pl.from_arrow(pq.read_table("./data/temple_details_spatial.parquet"))
        tract_nearest = pl.from_arrow(pq.read_table("./data/tract_distance_to_nearest_temple.parquet"))
        return tract, county, tract_nearest, temples
    
    def integrate_and_clean_data(self):
        # Merge Temples and County Data
        self.df_merged = self.df_temples.join(self.df_county, on=['STATEFP', 'COUNTYFP'], how='inner')
    
    def user_input(self):
        # User Input
        st.sidebar.title("Input Parameters")
        state = st.sidebar.selectbox(
            "State", 
            self.df_county.select('state_name').unique().sort('state_name').to_pandas()['state_name'].tolist()
        )
        
        county = st.sidebar.selectbox(
            "County",
            self.df_county.filter(col('state_name') == state).select('county_name').unique().sort('county_name').to_pandas()['county_name'].tolist()
        )

        self.selected_state = state
        self.selected_county = county


        self.selected_status = st.sidebar.selectbox(
            "Temple Status",
            ['ALL', 'ANNOUNCED', 'CONSTRUCTION', 'OPERATING']
        )

        if st.sidebar.button('Show Temples'):
            self.create_spatial_map(self.selected_state, self.selected_status)
            self.analyze_county_data()


    def analyze_county_data(self):
        county_data = self.df_county.filter((col('state_name') == self.selected_state) & (col('county_name') == self.selected_county))
        
        active_member_estimate = county_data['active_members_estimate'].sum()

        total_temples = len(self.df_temples.filter(col('stateRegion') == self.selected_state))

        # Display the results
        st.write(f"Active Membership in {self.selected_county}, {self.selected_state}: {active_member_estimate}")
        st.write(f"Total Number of Temples in {self.selected_state}: {total_temples}")

        df_plot = pd.DataFrame({
            'Total Temples in State': [total_temples],
            'Active Membership Estimate in County': [active_member_estimate]
        })

        fig = px.scatter(
            df_plot,
            x='Total Temples in State',
            y='Active Membership Estimate in County',
            title=f'Scatter Plot for {self.selected_state} State and {self.selected_county}'
        )

        st.plotly_chart(fig)



    def perform_analysis(self):
        # Temple Distribution by State
        self.temples_by_state = self.df_temples.group_by('stateRegion').count().rename({'stateRegion': 'state'})

        # Active Membership Estimates by State
        self.active_members_by_state = self.df_county.group_by('state_name').agg(pl.sum('active_members_estimate')).rename({'state_name': 'state'})

        self.comparison_df = self.temples_by_state.join(self.active_members_by_state, on='state', how='outer')

        self.comparison_df = self.comparison_df.with_columns([
            pl.col('count').fill_null(0).alias('temple_count'),
            pl.col('active_members_estimate').fill_null(0)
        ])

    def create_histogram(self):
        # Create a histogram to show the distribution of active membership estimates across all counties
        fig = px.histogram(
            self.df_county.to_pandas(),  
            x='active_members_estimate',
            title='Histogram of Active Membership Estimates Across All Counties',
            labels={'active_members_estimate': 'Active Membership Estimates'},
            nbins=50,  
            marginal='box'  
        )
        st.plotly_chart(fig)



    def perform_correlation_analysis(self):

        correlation = self.comparison_df.to_pandas()['temple_count'].corr(self.comparison_df.to_pandas()['active_members_estimate'])
        st.write('Correlation coefficient between number of temples and active membership estimates:', correlation)

    def create_visualizations(self):
        # Create the scatter plot
        fig = px.scatter(
            self.comparison_df.to_pandas(),
            x='temple_count', 
            y='active_members_estimate',
            hover_data=['state'], 
            title='Scatter Plot of Temples vs Active Membership Estimates by State',
            labels={
                'temple_count': 'Number of Temples',
                'active_members_estimate': 'Active Membership Estimates'
            }
        )

        st.plotly_chart(fig)

        self.create_histogram()
        self.perform_correlation_analysis()


    def get_temples_data(self, status="ALL"):
        if status != 'ALL':
            filtered_temples = self.df_temples.filter(col('status') == status)
        else:
            filtered_temples = self.df_temples
        
        temples_df_with_coordinates = pd.DataFrame({
            'latitude': filtered_temples['lat_general'],
            'longitude': filtered_temples['long_general'],
            'label': filtered_temples['temple'],
            'type': 'temple',
            'stateRegion': filtered_temples['stateRegion'],
            'city': filtered_temples['city'],
            'STATEFP': filtered_temples['STATEFP'],
            'COUNTYFP': filtered_temples['COUNTYFP'],
        })
        return temples_df_with_coordinates
    
    
    def create_spatial_map(self, state, status="ALL"):
        filtered_data = self.get_temples_data(status).query("stateRegion == @state")

        temple_layer = pdk.Layer(
            'ScatterplotLayer',
            filtered_data,
            get_position=['longitude', 'latitude'],
            get_color='[0, 0, 255, 140]',
            get_radius=10000,
            pickable=True
        )

        # Set the view state to focus on the selected state
        view_state = pdk.ViewState(
            latitude=filtered_data['latitude'].mean(),
            longitude=filtered_data['longitude'].mean(),
            zoom=6
        )

        # Render the map
        st.pydeck_chart(pdk.Deck(
            map_style='mapbox://styles/mapbox/light-v9',
            initial_view_state=view_state,
            layers=[temple_layer]
        ))
    
    def render_insight(self):
        st.subheader("Insights and Interpretation")
        st.write("""
        The analysis conducted on the placement of temples by state in relation to county active membership estimates provides several key insights:

        - **Spatial Distribution of Temples:** The interactive map visualization, which shows temple locations in a selected state, offers a clear picture of the geographical distribution of temples. This distribution can be compared against active membership estimates in different counties to assess whether temple placements align well with areas of high membership concentration.

        - **State-Level Analysis:** The correlation between the number of temples in a state and the aggregate active membership estimates in that state is a crucial metric. A positive correlation would suggest that temple placement is effectively aligned with areas of higher active membership. 

        - **County-Level Analysis:** For a more granular view, the analysis at the county level, especially in the selected state and county, provides insights into how local temple placements correspond to local active membership estimates. Significant discrepancies or alignments at this level offer valuable insights into the effectiveness of temple placement strategies.

        - **Scatter Plot Visualization:** The scatter plot comparing the number of temples to active membership estimates by state illustrates the relationship between these two variables. A linear or clustered pattern might indicate a strategic alignment of temple placements with membership densities, while scattered or sparse plots might suggest areas for improvement in temple distribution.

        - **Histogram Analysis:** The distribution of active membership estimates across all counties, as shown in the histogram, helps understand the general spread and concentration of active members. This analysis can be essential to identify counties with high membership but low temple counts, which could be potential areas for future temple developments.

        - **Correlation Analysis:** The calculated correlation coefficient between temple count and active membership estimates provides a quantitative measure of their relationship. A high correlation coefficient would reinforce the notion that temple placements are in sync with membership distributions.

        In conclusion, this analysis presents a comprehensive view of how current temple placements correlate with active membership estimates. It highlights areas where temple placement appears to be strategically aligned with membership densities, as well as areas where there may be opportunities for further alignment. Such insights are crucial for informed decision-making in future temple development and placement strategies.
        """)



if __name__ == "__main__":
    analysis = Analysis('./data/gz_2010_us_040_00_500k.json')
    analysis.load_geo_json_data()
    analysis.load_data()

    analysis.user_input()

    analysis.integrate_and_clean_data()
    analysis.perform_analysis()
    analysis.create_visualizations()

    analysis.get_temples_data("ALL")
    analysis.render_insight()

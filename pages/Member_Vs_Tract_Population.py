import streamlit as st 
import polars as pl 
import pyarrow.parquet as pq 
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import scipy.stats as stats



def load_data():
    tract = pl.read_parquet("./data/active_members_tract.parquet")
    temples = pl.from_arrow(pq.read_table("./data/temple_details_spatial.parquet"))
    tract_nearest = pl.from_arrow(pq.read_table("./data/tract_distance_to_nearest_temple.parquet"))
    return tract, tract_nearest, temples

tract, tract_nearest, temples = load_data()


def main():
    st.title("Community Tract Analysis")
    st.write("""
    This app provides an analysis of the estimated active members in relation to the population of each tract.
    The aim is to explore whether the active member estimates appear reasonable when compared to tract populations.
    """)

    # Displaying a sample of the data
    st.subheader("Data Sample")
    st.write("Below is a sample of the tract data being analyzed. It includes information on the population of each tract, the estimated number of active members, and their proportion.")
    st.write(tract.head())

    st.markdown("---")

    # Population Range Slider
    st.subheader("Filter by Population Range")
    st.write("Use the slider below to focus on tracts within a specific population range. This helps to examine if the active member estimates in these ranges are proportional.")
    population_range = st.slider(
        "Select Population Range",
        min_value=int(tract['population'].min()), 
        max_value=int(tract['population'].max()), 
        value=(int(tract['population'].min()), int(tract['population'].max()))
    )
    filtered_tract = tract.filter((tract['population'] >= population_range[0]) & (tract['population'] <= population_range[1]))


    # Scatter Plot for Population vs. Active Members Estimate
    st.subheader("Active Members vs. Population")
    st.write("The scatter plot visualizes the relationship between the population of each tract and the estimated number of active members. The trendline provides an overview of the general relationship pattern.")
    fig = px.scatter(filtered_tract.to_pandas(), x='population', y='active_members_estimate',
                     hover_data=['home'], trendline="ols",
                     labels={'population': 'Population', 'active_members_estimate': 'Active Members Estimate'},
                     title="Proportion of Active Members vs. Population")
    st.plotly_chart(fig)

    st.markdown("---")

    # Proportion Range Slider
    st.subheader("Filter by Proportion of Active Members")
    st.write("Adjust the slider to analyze tracts with specific ranges of active member proportions. This is useful for identifying tracts with unusually high or low active member estimates.")
    proportion_range = st.slider(
        "Select Proportion Range",
        min_value=float(tract['proportion'].min()),  
        max_value=float(tract['proportion'].max()),  
        value=(0.5, float(tract['proportion'].max()))  
    )

    # Filtering data based on the selected range
    filtered_tract_proportion = tract.filter((tract['proportion'] >= proportion_range[0]) & (tract['proportion'] <= proportion_range[1]))

    # Histogram for Proportion Distribution with Bell Curve
    st.subheader("Tract Distribution by Proportion")
    st.write("The histogram below shows the distribution of tracts by their proportion of active members. The overlaid normal distribution curve (bell curve) helps assess if the distribution follows a typical pattern.")
    hist_data = filtered_tract_proportion['proportion'].to_numpy()  # Convert to numpy array
    mean, std_dev = np.mean(hist_data), np.std(hist_data)

    # Create the histogram
    hist_fig = px.histogram(filtered_tract_proportion.to_pandas(), x='proportion', nbins=30,
                            title="Tract Count by Proportion Range")
    x = np.linspace(min(hist_data), max(hist_data), 100)
    y = stats.norm.pdf(x, mean, std_dev) * len(hist_data) * (max(hist_data) - min(hist_data)) / 30
    bell_curve = go.Scatter(x=x, y=y, mode='lines', name='Normal Distribution', line=dict(color='red'))
    hist_fig.add_trace(bell_curve)
    st.plotly_chart(hist_fig)


    st.markdown("---")

    # Insights with Enhanced Explanation
    st.subheader("Insights and Interpretation")
    st.write("""
    The scatter plot and histogram provide key insights into the reasonableness of active member estimates:

    - **Scatter Plot Analysis:** This plot shows the relationship between the population of each tract and the estimated number of active members. A clear trend or pattern here suggests that the active member estimates generally increase with population, as might be expected. Significant deviations from this trend could indicate irregularities or special cases in specific tracts.

    - **Histogram with Bell Curve Analysis:** The histogram displays the distribution of the proportions of active members in tracts. Overlaying this with a bell curve (normal distribution) serves a crucial purpose:
        - A distribution that closely follows the bell curve indicates that most tracts have active member proportions around the average, with fewer tracts showing very high or low proportions. This pattern is typical in many natural datasets and suggests that the estimates are 'reasonable' or expected.
        - If the distribution is skewed or does not follow the bell curve, it indicates anomalies. These are tracts where the proportion of active members is unusually high or low compared to the rest. Such cases may require further investigation to understand why they differ from the norm.
    
    In summary, 'reasonable estimates' here mean that the active member estimates in various tracts align with common statistical patterns, such as the normal distribution, and do not show unexpected extremes or irregularities.
    """)

main()
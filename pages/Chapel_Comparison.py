import streamlit as st
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import plotly.express as px
import os
from pathlib import Path

# Print current working directory
print("Current Working Directory:", os.getcwd())

def load_data():
    chapel_scrape = pl.read_parquet("./data/full_church_building_data-20.parquet")
    chapel_safegraph = pl.read_parquet("./data/safegraph_chapel.parquet")
    return chapel_scrape, chapel_safegraph


def count_unique_chapels(dataframe, unique_column):
    return dataframe.select(unique_column).unique().shape[0]

def plot_distribution(dataframe, column, title):
    counts = dataframe.groupby(column).count()
    fig = px.bar(counts.to_pandas(), x=column, y='count', title=title)
    return fig


# Load the data
chapel_scrape, chapel_safegraph = load_data()
chapel_scrape = chapel_scrape.with_columns([
    pl.col('state').str.to_uppercase().alias('state')
])



# Perform analyses and plot using Streamlit
st.title("Chapel Data Analysis")

# Unique chapels
st.subheader("Unique Chapel Counts")
unique_chapels_scrape = count_unique_chapels(chapel_scrape, 'place_id')
unique_chapels_safegraph = count_unique_chapels(chapel_safegraph, 'placekey')
st.write(f"Web Scrape Data: {unique_chapels_scrape} unique chapels")
st.write(f"Safegraph Data: {unique_chapels_safegraph} unique chapels")
st.markdown("""
*This section shows the count of unique chapels identified in each dataset. 
It helps in understanding the coverage and reach of the datasets.*
""")

# State-wise Distribution using an Interactive Map
st.header("State-wise Chapel Distribution on Map")


def plot_state_distribution(df, state_col, title):

    state_count = df.groupby(state_col).agg(counts=pl.count()).sort('counts').to_pandas()


    fig = px.choropleth(
        state_count, 
        locations=state_col, 
        locationmode="USA-states", 
        color='counts', 
        color_continuous_scale="Viridis",  
        scope="usa", 
        title=title,
        hover_data={state_col: False, 'counts': True}  # This will prevent state_col from being displayed in hover
    )

    # Update layout for better readability
    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        coloraxis_colorbar={
            'title':'Chapel Count'
        }
    )

    st.plotly_chart(fig)


plot_state_distribution(chapel_scrape, 'state', 'Chapel Distribution by State - Church Website Data')
plot_state_distribution(chapel_safegraph, 'region', 'Chapel Distribution by State - SafeGraph Data')

# Top States Analysis
st.header("Top States with Most Chapels")

def plot_top_states(df, state_col, title, top_n=10):
    top_states = df.groupby(state_col).count().sort("count").head(top_n)
    fig = px.bar(top_states, x=state_col, y='count', title=title)
    st.plotly_chart(fig)

plot_top_states(chapel_scrape, 'state', 'Top States - Church Website Data')
plot_top_states(chapel_safegraph, 'region', 'Top States - SafeGraph Data')



def plot_state_boxplot(df, state_col, title):
    # Group by state_col and count the occurrences
    state_count = df.groupby(state_col).agg(count=pl.count()).to_pandas()

    # Create a box plot
    fig = px.box(state_count, y='count', title=title)
    fig.update_yaxes(title='Number of Chapels')
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig)

st.header("Distribution of Chapels")
plot_state_boxplot(chapel_scrape, 'state', 'Distribution of Chapels - Church Website Data')
plot_state_boxplot(chapel_safegraph, 'region', 'Distribution of Chapels - SafeGraph Data')


# Insights with Enhanced Explanation
st.subheader("Insights and Interpretation")
st.write("""
The analysis of chapel data from both the church website web scrape and Safegraph datasets provides valuable insights into the distribution and presence of chapels across different regions:

- **Unique Chapel Counts Comparison:** The unique chapel counts from the web scrape and Safegraph datasets reveal the scope and coverage of each dataset. A higher count in one dataset may indicate a broader or more inclusive data collection approach. For example, a higher count in the Safegraph data might suggest its inclusion of less formal or smaller chapels not listed on official church websites.

- **State-wise Distribution Analysis:** By examining the interactive maps showing chapel distribution by state, we can observe the geographical spread and density of chapels in each dataset. Differences in these distributions might highlight regional biases or the varying effectiveness of data collection methods used by the two sources.

- **Top States Analysis:** Identifying states with the highest number of chapels in each dataset helps understand which regions are most prominently represented. Discrepancies in the representation of states between the two datasets could suggest localized differences in chapel presence or reporting.

- **Chapel Distribution Spread:** The box plot visualizations shed light on the variability and distribution spread of chapel counts across states. A wider spread in one dataset could indicate a greater variability in chapel presence, possibly due to different data collection or chapel establishment patterns.

In summary, comparing the Safegraph data to the church website web scrape data provides a comprehensive view of chapel distribution. While the web scrape offers a look at officially recognized chapels, Safegraph potentially broadens the perspective to include a wider range of chapel locations. This comparative analysis helps in understanding not just the distribution of chapels, but also the nuances and potential biases in the datasets. It underscores the importance of considering multiple sources for a more complete picture of chapel presence across regions.
""")


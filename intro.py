import streamlit as st

def main():
    st.title("Temple and Membership Analysis App")
    
    st.write("""
    ### Welcome to the Temple and Membership Analysis App!

    This interactive app provides a comprehensive analysis of temple placements in relation to active membership estimates across various states and counties.

    **Key Features of the App:**

    - **Geographical Visualization:** Explore interactive maps showing the distribution of temples in selected states and counties.
    - **State and County Level Analysis:** Delve into detailed analyses at both the state and county levels, comparing temple counts with active membership estimates.
    - **Dynamic Data Exploration:** Interact with various data visualizations, including scatter plots, histograms, and correlation matrices to uncover insights into temple placement and membership trends.
    - **User-Driven Exploration:** Customize your analysis by selecting specific states and counties, and filter temples based on their status (e.g., Announced, Under Construction, Operating).

    **How to Use the App:**

    - Just select the Page you want to view from the sidebar on the left.

    The app aims to provide valuable insights for strategic temple placement and to understand the dynamics of temple memberships across different regions.

    Get started by making your selections in the sidebar!
    """)

if __name__ == "__main__":
    main()

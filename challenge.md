## DATA &#8614; DASHBOARD

We learned [Streamlit](https://streamlit.io/) and [Docker](https://www.docker.com/) as two tools to use in the data science process. We will use these two tools to create an interactive app that allows users to explore the reliability of our membership activity estimates as developed in Pyspark on Databricks.

We will leverage our member activity estimates, temple locations, and church building locations to allow our team to complete data validation. We are validating our programming (specifically Hathaway's) and SafeGraph's data quality.

## Technologies for coding challenge

- __Required:__ [Streamlit Dashboard](https://streamlit.io/)
- __Required:__ [Docker](https://www.docker.com/)
- __Required:__ [Polars](https://pola-rs.github.io/polars/py-polars/html/index.html)
- __Optional:__ [Plotly Express](https://plotly.com/python/plotly-express/)
- __Optional:__ [PySpark](https://spark.apache.org/docs/latest/api/python/#:~:text=PySpark%20is%20an%20interface%20for,data%20in%20a%20distributed%20environment.) and [DataBricks](https://databricks.com/)


## Coding Challenge

This coding challenge will take some time. You should plan on regular work on this challenge throughout the next two weeks.  If you start on this project a few days before it is due, you will most likely fail. You can use Google searches and AI to complete your work.  You cannot use other humans. You can ask the teacher and TA questions on Slack.

- __App Challenge:__ Build a Streamlit App in a Docker Container and store the files in this repo so others can run your app.
- __Code Evaluation Challenge:__ Review Hathaway's Target Development Code to provide improvements and document the process used.
- __Feature Challenge:__ Complete your simple and complex feature using Pyspark on Databricks with the unit of analysis as a tract.
- __Vocabulary Challenge:__ Review the text below and answer the questions.

Please read below for details on how to complete each challenge area.

### App Challenge

Please address the following topics in your app. You can explore answers to these questions by state, county, and/or tract.

1. How does the number of chapels in Safegraph compare to the number of chapels from the church website web scrape?
2. Does the active member estimate look reasonable as compared to the tract population?
3. Does the active member estimate look reasonable as compared to the religious census estimates by county?
4. How does the current temple placement look by state as compared to the county active membership estimates?

At a minimum, your app should have the following elements.

1. The ability to explore answers to these questions using an interactive filter with the state variable.
2. Spatial map with membership and temples shown on the map.
3. Tract distribution charts (boxplots and histograms) based on selected counties within a state.
4. The option to show the temples or hide the temples on the map.
5. A scaling input that allows the user to input a value between 0 and 1 that will adjust the active membership estimates proportionally.

#### Data Science Dashboard

We will use Streamlit as our prototype dashboard tool, but we need to embed that streamlit app into a Docker container.

Within this repository, you can simply run `docker compose up` to leverage the `docker-compose.yaml` with your local folder synced with the container folder where the streamlit app is running. 

Additionally, you can use `docker build -t streamlit .` to use the `Dockerfile` to build the image and then use `docker run -p 8501:8501 -v "$(pwd):/app:rw" streamlit` to start the container with the appropriate port and volume settings.

### Repo Structure

Your repo should be built so that I can clone the repo and run the Docker command (`docker compose up`) as described in your `readme.md`. This will allow me to see your app in my web browser without requiring me to install Streamlit on my computer.

1. Fork this repo to your private space
2. Add me to your private repo in your space (`hathawayj`)
3. Build your app and Docker container
4. Update your `readme.md` with details about your app and how to start it.
5. Include a link in your `readme.md` to your GitHub repository.


### Code Evaluation Challenge

_Within your `readme.md` file in your repository and as a submitted `.pdf` or `.html` on Canvas, address the following items:_

You will review the `active_membership_disperse` Databricks files and document the following items. I am exploring your ability to digest and explain Pyspark code.

1. Explain what the code is doing in `Cmd`s 5, 22, 50, 60, 64, 66, and 74. Each `Cmd` will most likely require a couple of paragraphs. Please be detailed, but don't just write sentences describing each line.
2. Include an additional paragraph for each `Cmd` that proposes code improvements and any bugs that may be present.

### Feature Challenge

_Within your `readme.md` file in your repository and as a submitted `.pdf` or `.html` on Canvas include one spatial map of your feature and one feature vs target chart._ Note your charts could also be included in your app.

Include commented and documented code as exported from Databricks as `.html` and `.ipynb` files.

### Vocabulary/Lingo Challenge

_Within your `readme.md` file in your repository and as a submitted `.pdf` or `.html` on Canvas, address the following items:_

1. Explain the added value of using DataBricks in your Data Science process (using text, diagrams, and/or tables).
2. Compare and contrast PySpark to either Pandas or the Tidyverse (using text, diagrams, and/or tables).
3. Explain Docker to somebody intelligent but not a tech person (using text, diagrams, and/or tables).

_Your answers should be clear, detailed, and no longer than is needed. Imagine you are responding to a client or as an interview candidate._

- _Clear:_ Clean sentences and nicely laid out format.
- _Detailed:_ You touch on all the critical points of the concept. Don't speak at too high a level.
- _Brevity:_ Don't ramble. Get to the point, and don't repeat yourself.



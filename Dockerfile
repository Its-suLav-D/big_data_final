FROM python:3.11.6-slim
EXPOSE 8501
WORKDIR /app
COPY . .
RUN pip3 install -r requirements.txt
ENTRYPOINT ["streamlit", "run", "intro.py", "--server.port=8501", "--server.address=0.0.0.0"]

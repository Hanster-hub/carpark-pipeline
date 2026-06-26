FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit pandas requests sqlalchemy psycopg2-binary
COPY common ./common
COPY dashboard ./dashboard
ENV PYTHONPATH=/app
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
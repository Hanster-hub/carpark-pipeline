FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" mlflow==2.13.0 scikit-learn pandas
COPY common ./common
COPY api ./api
COPY ml ./ml
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

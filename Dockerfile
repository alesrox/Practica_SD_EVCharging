FROM python:3.12-slim
RUN pip install --no-cache-dir confluent-kafka
COPY topics.py /create_topics.py
CMD ["python", "/create_topics.py"]
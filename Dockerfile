FROM python:3.11-slim
RUN pip install --no-cache-dir confluent-kafka
COPY topics.py /create_topics.py
CMD ["python", "/create_topics.py"]
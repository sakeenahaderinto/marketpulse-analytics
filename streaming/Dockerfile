FROM apache/spark:3.5.3-python3

USER root
# Install psycopg2-binary or other python deps if needed
RUN pip install --no-cache-dir python-dotenv

# Switch to spark user
USER 1001
WORKDIR /opt/spark/work-dir

COPY streaming/ /opt/spark/work-dir/streaming/

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3", \
     "streaming/process_quotes.py"]
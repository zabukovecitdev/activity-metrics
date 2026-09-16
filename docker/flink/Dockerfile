FROM flink:1.19.1-scala_2.12-java11

# PyFlink needs a CPython 3.8-3.11 interpreter available as `python`/`python3`,
# which the upstream Flink image does not ship.
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Keep the PyFlink version in lockstep with the base Flink image version above.
RUN pip3 install --no-cache-dir apache-flink==1.19.1

# Kafka SQL/Table & DataStream connector, so PyFlink jobs can read/write Kafka
# topics without each job having to ship its own jar.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/

WORKDIR /opt/flink

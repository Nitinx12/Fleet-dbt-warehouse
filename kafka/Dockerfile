FROM spark:4.3.0-scala2.13-java21-python3-ubuntu

ARG GIT_SHA=unknown
LABEL org.opencontainers.image.title="Fleet stream job" \
      org.opencontainers.image.revision="${GIT_SHA}"

USER root
ENV PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    PYTHONPATH=/repo:/repo/kafka \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    SPARK_CONF_DIR=/opt/spark/conf \
    PATH=/opt/venv/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv==0.11.18 \
    && mkdir -p /opt/spark/jars /var/lib/fleet-stream/checkpoints

WORKDIR /repo
COPY pyproject.toml uv.lock ./
RUN UV_PYTHON=3.12 uv sync --frozen --no-dev --no-install-project --only-group kafka --only-group spark

COPY utils /repo/utils
COPY scripts/stream_admin.py scripts/stream_job.py scripts/stream_producer.py scripts/stream_monitor.py scripts/stream_reconcile.py /repo/scripts/
COPY contracts/streams /repo/contracts/streams
COPY sql/scripts /repo/sql/scripts
COPY dbt/models/bronze /repo/dbt/models/bronze
COPY kafka /repo/kafka
COPY kafka/config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.3.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.3.0/spark-sql-kafka-0-10_2.13-4.3.0.jar \
    && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.3.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.3.0/spark-token-provider-kafka-0-10_2.13-4.3.0.jar \
    && curl -fsSL -o /opt/spark/jars/kafka-clients-3.9.2.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
    && curl -fsSL -o /opt/spark/jars/scala-parallel-collections_2.13-1.2.0.jar https://repo1.maven.org/maven2/org/scala-lang/modules/scala-parallel-collections_2.13/1.2.0/scala-parallel-collections_2.13-1.2.0.jar \
    && curl -fsSL -o /opt/spark/jars/jsr305-3.0.0.jar https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar \
    && curl -fsSL -o /opt/spark/jars/commons-pool2-2.13.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar \
    && curl -fsSL -o /opt/spark/jars/postgresql-42.7.14.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    && chown -R spark:spark /opt/venv /opt/uv-python /repo /var/lib/fleet-stream

USER spark
WORKDIR /repo
CMD ["python", "/repo/scripts/stream_job.py"]

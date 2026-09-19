FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt
COPY scripts/spark_entrypoint.sh /usr/local/bin/spark-entrypoint
RUN chmod +x /usr/local/bin/spark-entrypoint && mkdir -p /workspace /data /models /checkpoints && chown -R 185:185 /workspace /data /models /checkpoints

USER 185
WORKDIR /workspace

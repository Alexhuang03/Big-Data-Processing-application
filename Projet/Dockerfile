# Spark + Jupyter image with the Python libraries the project needs.
# Pinned: the PYTHONPATH in compose.yaml depends on the py4j version of this image.
FROM quay.io/jupyter/pyspark-notebook:spark-4.2.1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

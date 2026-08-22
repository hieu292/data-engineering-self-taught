# Phase 0: notebook tối giản — đóng vai "Databricks Notebook".
# Các phase sau sẽ thêm pyspark, delta-spark, dbt... vào chính file này.
FROM quay.io/jupyter/minimal-notebook:python-3.13

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    fix-permissions "${CONDA_DIR}" && fix-permissions "/home/${NB_USER}"

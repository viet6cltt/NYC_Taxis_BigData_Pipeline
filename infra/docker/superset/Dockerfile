FROM apache/superset:latest

# The official image runs Superset from /app/.venv, so dependencies must land
# in that environment rather than the global interpreter.
USER root
RUN uv pip install --python /app/.venv/bin/python \
    psycopg2-binary \
    trino \
    sqlalchemy-trino \
    fastmcp
USER superset

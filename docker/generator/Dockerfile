FROM python:3.12-slim
WORKDIR /app
COPY generator/pyproject.toml ./
COPY generator/blogsim ./blogsim
RUN pip install --no-cache-dir . && useradd -m gen
USER gen
ENTRYPOINT ["blogsim"]
CMD ["--help"]

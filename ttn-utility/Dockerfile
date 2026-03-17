FROM python:3.14-slim

WORKDIR /code

RUN pip install poetry

# Let poetry install into system python
RUN poetry config virtualenvs.create false

# Copy only poetry files first (to help caching)
COPY pyproject.toml poetry.lock* ./
RUN poetry install --only main

COPY . .
# This copies from a secondary context called "common", normally we cannot access files outside of our context
COPY --from=common . ./common

ENTRYPOINT ["python", "app.py"]

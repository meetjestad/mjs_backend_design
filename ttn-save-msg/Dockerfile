# 3.12 breaks pony: https://github.com/ponyorm/pony/issues/694
FROM python:3.11

RUN pip install poetry

# Install dependencies before adding all code to not invalidate docker
# layer cache when code changes. See
# https://medium.com/@albertazzir/blazing-fast-python-docker-builds-with-poetry-a78a66f5aed0
WORKDIR /code
COPY pyproject.toml .
COPY poetry.lock .
RUN poetry install --without dev --no-root --no-cache

COPY . .
# This copies from a secondary context called "common", normally we cannot access files outside of our context
COPY --from=common . ./common

CMD ["poetry", "run", "python", "app.py"]

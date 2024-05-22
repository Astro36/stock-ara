FROM python:3.12-alpine AS requirements

WORKDIR /tmp

RUN pip install poetry
COPY ./pyproject.toml ./poetry.lock /tmp/
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes


FROM python:3.12-alpine

WORKDIR /code

COPY --from=requirements /tmp/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . /code/

CMD ["python", "-m", "stock_ainalyst.main"]

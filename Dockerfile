# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

WORKDIR /usr/src/river

RUN pip install poetry==1.6.1 && poetry config virtualenvs.create false

COPY pyproject.toml /usr/src/river/
COPY poetry.lock /usr/src/river/


RUN poetry install

COPY ./ /usr/src/river/

# Update package lists, install nodejs, npm, and protoc
RUN apt-get update && \
  apt-get install -y nodejs npm protobuf-compiler && \
  npm install -g pnpm
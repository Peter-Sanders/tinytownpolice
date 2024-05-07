# syntax=docker/dockerfile:1

FROM jupyter/all-spark-notebook

RUN pip install delta-spark

RUN git clone https://github.com/Peter-Sanders/tinytownpolice.git


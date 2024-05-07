# syntax=docker/dockerfile:1

FROM jupyter/all-spark-notebook

# ENV CONDA_DIR /opt/conda
# RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
#     /bin/bash ~/miniconda.sh -b -p /opt/conda
#
# ENV PATH=$CONDA_DIR/bin:$PATH

# COPY ttpd_env.yml 
COPY . .

# RUN conda env create -f ttpd_env.yml
#
# RUN echo "conda activate ttpd" >> ~/.bashrc

# COPY solution.ipynb
# COPY ttpd_data.zip



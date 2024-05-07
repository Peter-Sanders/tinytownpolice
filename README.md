# tinytownpolice

steps to startup [work in progress still]

# This needs to be rewritten if i get docker to play nice
docker run -p 10000:8888 jupyter/all-spark-notebook

in a new terminal, run docker ps and grab the containerid

docker cp ./ttpd_data <containerid>:/home/ttpd_data

navigate to localhost:10000

grab the token from the startup terminal and paste it to login


# This is the version that works locallly, but the conda parts really stink so it would be nice to get this all in docker
Steps to startup:

1. Create the virtual environmnet
    - ensure you at least have python and conda installed
    - execute conda env create -f environment.yml
    - after completion, activate via "conda activate ttpd"
2. Start JupyterLab
    - execute "jupyter lab" in a terminal

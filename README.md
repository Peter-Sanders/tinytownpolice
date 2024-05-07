# tinytownpolice

## How to Deploy
1. Create the Docker Image
    - docker build --tag=ttpd .
2. Run the container and forward 8888 through
    - docker run -p 8888:8888 ttpd 
3. Follow the link to localhost:8888, apply the token, and enter JupyterLab


### Installation Notes
- ttpd_env.yml is left for posterity. If the conda env needs to be created from scratch because Docker isn't available for some reason.
- The only non spark and delta dependencies for this project are pandas and matplotlib, all others are from the standard library
- delta is the only dependency not captured in the base docker image. It also must be installed via pip not conda.



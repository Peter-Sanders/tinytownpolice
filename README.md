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
- if required, since delta isn't depended upon other than to create the sparkSession, it can be removed from the repo entirely
    - in the notebook, remove the import from the imports block
    - comment out or delete the two .config() lines, remembering to remove the trailing '/' from the preceeeding appname section
    - modify the return in create_spark_session to be 
        return builder.getOrCreate()
    - it would also be prudent to modify the Dockerfile to turn off the step that installs delta via pip 
- The build process for the project docker image will checkout the master branch of this repo and pull it into the eventual container 
- main.py is (should) be an exact copy of the code in solution.ipynb. It represents what would be delivered in a more production-like environment where this report would likely be automated; kicked off via cron or a similiar scheduler and the resulting analysis/logging/images emailed off via mailx or the like.

## Assumptions
1. Data Arrives at some interval always in a zip file
2. The format mappings (i.e. people=csv, speeding=json, auto=xml) will not change
3. The schema within each datasource format will not change
4. A Docker container is an acceptable means of delivery


## Documentation
1. while not included in the docker container, documentation for this project was created by utilizing the pdoc3 module
2. html/main.html contains said documentation
3. to recreate, install pdoc3 then run 'pdoc --html main' optionally add '--force' to rebuild an existing main.html

# tinytownpolice

## How to Deploy
1. Create the Docker Image
    - docker build --no-cache --tag=ttpd .
2. Run the container and forward 8888 through
    - docker run -p 8888:8888 ttpd 
3. Follow the link to localhost:8888, apply the token, and enter JupyterLab


### Installation Notes
- ttpd_env.yml is left for posterity. If the conda env needs to be created from scratch because Docker isn't available for some reason.
- The only non spark dependencies for this project are pandas and matplotlib, all others are from the standard library
- The build process for the project docker image will checkout the main branch of this repo and pull it into the eventual container 
- main.py is (should) be an exact copy of the code in solution.ipynb. It represents what would be delivered in a more production-like environment where this report would likely be automated; kicked off via cron or a similiar scheduler and the resulting analysis/logging/images emailed off via mailx or the like.
- There's some fun nuance when running matplotlib off on a separate thread. 
    - When running a notebook, the run_type global var should be "jupyter". 
    - If running main.py from the terminal, set it to "python"
    - Kinda weird to have this distinction. In a produciton environment where the entrypoint would likely be main.py,I would just completely turn off verbose matplotlib stuff and only dump it to img.


## To Run 
1. solution.ipynb or main.py
2. if running from jupyter, just restart the kernal so all cells are activated in sequence


## Assumptions
1. Data Arrives at some interval always in a zip file
2. The format mappings (i.e. people=csv, speeding=json, auto=xml) will not change
3. The schema within each datasource format will not change
4. A Docker container is an acceptable means of delivery


## Documentation
1. While not included in the docker container, documentation for this project was created by utilizing the pdoc3 module
2. html/main.html contains said documentation
3. To recreate, install pdoc3 then run 'pdoc --html main' optionally add '--force' to rebuild an existing main.html
4. Linting was performed with pylint and was dumped to lint.txt, hence its exclusion in the gitignore

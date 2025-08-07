1. open dagster project in IDE

2. etl.yml file:
Execute below commands in terminal and activate the etl.yml file.
    conda env create -f etl.yml
    conda activate etl

3. config.json file:
Open the json file and update your MongoDB and Postgres credientials.

4. dagster pipeline execution:
Use the following command in terminal to execute the pipeline.
    dagit -f NCHS_etl.py

5. browser interaction:
Navigate to the http://127.0.0.1:3000/ url.
Click on Launchpad tab and click Launch Run Button.
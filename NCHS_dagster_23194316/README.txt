1. open dagster project in IDE

================================================================

2. etl.yml file: Only execute if you dont have all the necessary dependencies/libraries.

Execute below commands in terminal and activate the etl.yml file.
    conda env create -f etl.yml
    conda activate etl
if any "library not found" error occurs then kindly install that library.

================================================================

3. dagster pipeline execution: 
    - traverse to NCHS_dagster_23194316 folder
    - Use the following command in terminal to execute the pipeline.
        - dagit -f NCHS_etl.py

================================================================

4. browser interaction:
    - Navigate to the http://127.0.0.1:3000/ url.
    - Click on Launchpad tab and click Launch Run Button.

================================================================
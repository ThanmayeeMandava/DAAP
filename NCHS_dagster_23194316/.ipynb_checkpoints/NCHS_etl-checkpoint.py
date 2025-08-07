from dagster import job
#from NCHS_operations import *
from NCHS_operations import *

@job
def etl():
    visualize(
        fetch_data_from_postgresql(
            load_data_into_postgresql(
                transform_data(
                    extract_data_from_mongodb(
                        load_data_to_mongodb()
                    ) 
                )
            )
        )
)    
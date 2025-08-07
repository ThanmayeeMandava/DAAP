# Import Required Libraries
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from dagster import op, Out, In, get_dagster_logger
from plotly.subplots import make_subplots
from sqlalchemy.orm import sessionmaker
import plotly.graph_objects as go
from pymongo import MongoClient
import plotly.express as px
import pandas as pd
import requests
import json
import os

# Loading config function to use correct credentials
def load_config():
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config

# Using the config to load credentials
config = load_config()
MONGO_URI = config['mongo']['uri']
PG_DB_NAME = config['postgres']['dbname']
PG_USER = config['postgres']['user']
PG_PASSWORD = config['postgres']['password']
PG_HOST = config['postgres']['host']
PG_PORT = config['postgres']['port']

# Assigning the database and collection names 
db_name = "Group_O"
collection_name = "NCHS_data_23194316"

# Loading the logger to monitor the changes in the dagster logger
logger = get_dagster_logger()


# # 1. Load data to MongoDB Operation
# # creating the dagster operation to Fetching data from an API into MongoDB

# @op(out=Out(bool))
# def load_data_to_mongodb() -> bool:
#     # URL to fetch the data from
#     url = "https://data.cdc.gov/resource/xbxb-epbu.json"
#     result = False

#     # Connecting to MongoDB and creating the db and collection instances
#     client = MongoClient(MONGO_URI)  # Replace MONGO_URI with your actual MongoDB URI
#     db = client[db_name]  # Replace db_name with your actual database name
#     collection = db[collection_name]  # Replace collection_name with your actual collection name

#     try:
#         # Drop the collection if it exists
#         db.drop_collection(collection_name)
#         logger.info(f"Dropped existing collection '{collection_name}'.")

#         # Fetch data from the website
#         response = requests.get(url)
#         response.raise_for_status()  # This will raise an error if the fetch was unsuccessful
#         data = response.json()

#         # Check if data is a list of dictionaries and insert into MongoDB
#         if isinstance(data, list) and len(data) > 0:
#             collection.insert_many(data)
#         else:
#             logger.error("Data fetched is empty or not in expected format.")
#             return False
        
#         logger.info("Data successfully fetched from the website and inserted into MongoDB.")
#         result = True
#     except Exception as e:
#         logger.error(f"Error fetching or inserting data: {e}")
#         result = False
#     finally:
#         client.close()

#     return result

@op(out=Out(bool))def load_data_to_mongodb() -> bool:     # URL to fetch the data from    
    base_url = "https://data.cdc.gov/resource/xbxb-epbu.json"    
    limit = 1000  # Adjust if the API allows more    offset = 0    
    result = False# Connecting to MongoDB and creating the db and collection instances    
    client = MongoClient(MONGO_URI)  # Replace MONGO_URI with your actual MongoDB URI    
    db = client[db_name]  # Replace db_name with your actual database name    
    collection = db[collection_name]  # Replace collection_name with your actual collection nametry:         # Drop the collection if it exists        
    db.drop_collection(collection_name)         
    logger.info(f"Dropped existing collection '{collection_name}'.")         # Continue fetching data until there is no data left to fetchwhile True:             # Construct the URL with offset and limit            
    url = f"{base_url}?$limit={limit}&$offset={offset}"            
    response = requests.get(url)             
    response.raise_for_status()  # This will raise an error if the fetch was unsuccessful            
    data = response.json()             # Check if data is a list of dictionariesif isinstance(data, list) and len(data) > 0:                 collection.insert_many(data)                 offset += len(data)  # Move the offset                logger.info(f"Fetched and inserted {len(data)} records successfully.")             else:                 # If no data is returned, we're donebreak         logger.info("All data successfully fetched from the website and inserted into MongoDB.")         result = Trueexcept Exception as e:         logger.error(f"Error fetching or inserting data: {e}")         result = Falsefinally:         client.close()     return result

# 2. Extract Data from MongoDB Operation
# This operation fetches the data from MongoDB and performs initial filtering if needed.
@op(
    ins={"data_loaded": In(bool)} ,
    out=Out(pd.DataFrame)
     # Adding an input dependency
)
def extract_data_from_mongodb(data_loaded):# -> pd.DataFrame:
    # utilizing the data_loaded boolean more explicitly to decide 
    # whether or not to proceed with fetching data from MongoDB.
    if not data_loaded:
        logger.error("Data not loaded into MongoDB. Skipping extraction.")
        return pd.DataFrame()  # Return an empty DataFrame if loading was unsuccessful.

    try:
        client = MongoClient(MONGO_URI)
        db = client[db_name]
        collection = db[collection_name]
        cursor = collection.find({})
        df = pd.DataFrame(list(cursor))
        logger.info("Data extracted from MongoDB")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error


# 3. Data Transformation Operation
# This operation will clean and transform the data according to the specifications.

@op(
    ins={"df": In(pd.DataFrame)}, 
    out=Out(pd.DataFrame)
)
def transform_data(df) -> pd.DataFrame:
    try:

#----------------------------------------------------------------------------------------

        # Transformation 1: Standardize state names
        df['state'] = df['state'].str.title()
        logger.info("Standardized state names.")

#----------------------------------------------------------------------------------------

        # Transformation 2: Convert data types
        numeric_columns = ['deaths', 'population', 'crude_death_rate', 'age_adjusted_rate']
        for col in numeric_columns:
            original_non_numeric = df[col].apply(lambda x: not str(x).isnumeric()).sum()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            logger.info(f"Converted {col} to numeric, replaced {original_non_numeric} non-numeric values.")

#----------------------------------------------------------------------------------------

        # Transformation 3: Fill missing values
        if df['age_adjusted_rate'].isnull().any():
            df['age_adjusted_rate'] = df['age_adjusted_rate'].fillna(df['age_adjusted_rate'].mean())
            logger.info("Filled missing values in 'age_adjusted_rate'.")

#----------------------------------------------------------------------------------------

        # # Transformation 3: Calculate mortality rate
        # if 'mortality_rate' not in df.columns:
        #     df['mortality_rate'] = (df['deaths'] / df['population']) * 100000
        #     logger.info("Calculated mortality rate.")

#----------------------------------------------------------------------------------------

        # Transformation 4: Clip negative values
        for col in numeric_columns:
            df[col] = df[col].clip(lower=0)
        logger.info("Clipped negative values.")

#----------------------------------------------------------------------------------------

        # Transformation 5: Validate year range
        df['year'] = df['year'].astype(int)
        df = df[df['year'].between(1900, 2100)]
        logger.info("Filtered records by year range.")

#----------------------------------------------------------------------------------------

        logger.info("Data transformation complete.")
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        raise  # Optionally re-raise to ensure the pipeline is aware of the failure

    return df



# 4. Load Data into PostgreSQL Operation
# This operation will load the cleaned and transformed data into PostgreSQL.

# Configuration and Base setup
Base = declarative_base()

def init_pg_session():
    connection_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB_NAME}"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session, engine

class NCHSData(Base):
    __tablename__ = 'NCHS_data_23194316'
    _id = Column(String(255), primary_key=True)
    state = Column(String(255))
    year = Column(Integer)
    sex = Column(String(255))
    age_group = Column(String(255))
    race_and_hispanic_origin = Column(String(255))
    deaths = Column(Float)
    population = Column(Float)
    crude_death_rate = Column(Float)
    age_adjusted_rate = Column(Float)
    #mortality_rate = Column(Float)
    us_crude_rate = Column(Float)
    us_age_adjusted_rate = Column(Float)
    unit = Column(String(255))

@op(ins={"df": In(pd.DataFrame)}, out=Out(bool))
def load_data_into_postgresql(df: pd.DataFrame) -> bool:
    session, engine = init_pg_session()
    Base.metadata.create_all(bind=engine)

    try:
        df['_id'] = df['_id'].astype(str)
        column_names = [column.name for column in NCHSData.__table__.columns]
        df = df[column_names]
        df.to_sql(NCHSData.__tablename__, con=engine, if_exists='append', index=False, method='multi')
        logger.info("Data successfully loaded into PostgreSQL using the ORM")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading data into PostgreSQL: {e}")
        return False
    finally:
        session.close()

# 5. fetch Data from PostgreSQL Operation
# This operation will fetch the cleaned and transformed data from PostgreSQL.       

@op(
    ins={"data_loaded": In(bool)}, 
    out=Out(pd.DataFrame)
)
def fetch_data_from_postgresql(data_loaded: bool) -> pd.DataFrame:
    if not data_loaded:  # Check if the previous operation indicated successful data loading
        logger.error("Data not loaded into PostgreSQL. Cannot fetch data.")
        return pd.DataFrame()  # Return an empty DataFrame if loading was unsuccessful

    # Initialize the session and engine
    session, engine = init_pg_session()  # Assume init_pg_session does not require any arguments

    try:
        # Fetch data using SQLAlchemy ORM
        query = session.query(NCHSData)
        df = pd.read_sql(query.statement, session.bind)  # Use the session's bound engine to execute the query
        logger.info(f"Fetched {len(df)} records from PostgreSQL")
    except Exception as e:
        logger.error(f"Error fetching data from PostgreSQL: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame in case of an error
    finally:
        session.close()  # Ensure the session is closed properly

    return df

# # 6. visualize Operation
# # This operation will visualize cleaned and transformed data from PostgreSQL to plotly. 

@op(ins={'df': In(pd.DataFrame)}, out=Out(bool))
def visualize(df):
    try:
        if df.empty:
            logger.error("Received an empty DataFrame.")
            return False

        # Create a subplot grid: 2 rows, 3 columns with the second row having only one column for the line plot
        fig = make_subplots(
            rows=2, cols=3,
            specs=[[{"type": "pie"}, {"type": "box"}, {"type": "pie"}],
                   [{"type": "scatter", "colspan": 3}, None, None]],
            subplot_titles=('Top 5 States by Average Crude Death Rate', 'Crude Death Rate by Sex',
                            'Death Distribution by Age Group', 'Trends For top 5 states by Deaths over Years'),
            vertical_spacing=0.1  # Adjust space between the subplot rows
        )

#----------------------------------------------------------------------------------------

        # Visualization 1: Pie chart for state crude death rates
        state_crude_rate = df.groupby('state')['crude_death_rate'].mean().nlargest(5).reset_index()
        pie1 = px.pie(state_crude_rate, values='crude_death_rate', names='state')
        pie1.update_traces(textposition='inside', textinfo='label+percent', showlegend=False)
        for trace in pie1.data:
            fig.add_trace(trace, row=1, col=1)

#----------------------------------------------------------------------------------------

        # Visualization 2: Line plot for yearly death trends
        # Determine the top 5 states with the highest total deaths
        top_states = df[df['state'] != 'United States'].groupby('state')['deaths'].sum().nlargest(5).index

        # Filter the DataFrame to include only data from the top 5 states
        df_top_states = df[df['state'].isin(top_states)]

        # Now sort this filtered DataFrame
        df_line_plot = df_top_states.sort_values(by=['state', 'year'])

        # Create the line plot with Plotly Express for the top 5 states
        line = px.line(df_line_plot, x='year', y='deaths', color='state', markers=True)

        # Add the line plot trace to the subplot figure (fig)
        for trace in line.data:
            fig.add_trace(trace, row=2, col=1)

#----------------------------------------------------------------------------------------

        # Visualization 3: Box plot for crude death rate by sex
        box = px.box(df[df['sex'] != 'Both Sexes'], x='crude_death_rate', y='sex', color='sex')
        box.update_traces(showlegend=False)
        for trace in box.data:
            fig.add_trace(trace, row=1, col=2)

#----------------------------------------------------------------------------------------

        # Visualization 4: Donut chart for death distribution by age group
        age_group_data = df.groupby('age_group')['deaths'].sum().reset_index()
        donut = px.pie(age_group_data, values='deaths', names='age_group', hole=0.4)
        donut.update_traces(textposition='inside', textinfo='label+percent', showlegend=False)
        for trace in donut.data:
            fig.add_trace(trace, row=1, col=3)

#----------------------------------------------------------------------------------------

        # Update layout with the main title
        fig.update_layout(
             title={
                    'text': "Analysis of NCHS Drug Poisoning Data",
                    'y':0.95,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': {'size': 20, 'color': 'black', 'family': "Arial, sans-serif"}
                },
            height=700,  # May adjust height to better fit your view
            width=1500,  # May adjust width to better fit your view
            
        )

        # display the plots in the browser window
        fig.show()
        # Define the directory name
        directory = "visualizations"

        # Create the directory if it does not exist
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Save the combined plot in the 'visualizations' directory
        fig.write_image(os.path.join(directory, "Analysis of NCHS Drug Poisoning Data.png"))

        # Save individual plots in the 'visualizations' directory
        pie1.write_image(os.path.join(directory, "Top 5 States by Average Crude Death Rate.png"))
        line.write_image(os.path.join(directory, "Trends For top 5 states by Deaths over Years.png"))
        box.write_image(os.path.join(directory, "Crude Death Rate by Sex.png"))
        donut.write_image(os.path.join(directory, "Death Distribution by Age Group.png"))

        # If you are running this code in a script, the directory should be created in the same location as the script.
        # If you are running this in a Jupyter notebook, the directory will be created in the notebook's current working directory.


        logger.info("Visualizations created, displayed, and saved successfully.")
        return True
    except Exception as e:
        logger.error(f"Error creating visualization: {e}")
        return False



#--------------3rd combined plot --------------------------------

#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
from sqlalchemy import create_engine,text
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# DB connection
src_db = 'postgresql://postgres:postgres@pgdb:5432/Olympics'
trg_db = 'postgresql://postgres:postgres@pgdb:5432/OlympicsDW'

src_engine = create_engine(src_db)
trg_engine = create_engine(trg_db)


# In[6]:


def extract(query, engine):
    """Extract data from the source database."""
    try:
        df = pd.read_sql_query(query, con=engine)
        logging.info(f"Data extracted successfully for query: {query}")
        return df
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

def load(df, table_name, engine):
    """Load data into the target database."""
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded into {table_name}")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise


# In[7]:


# Extract source data
global_population_df = extract('SELECT * FROM globalpopulation', src_engine)
life_expectancy_df = extract('SELECT * FROM lifeexpectancy', src_engine)
list_of_countries_df = extract('SELECT * FROM listofcountriesareasbycontinent', src_engine)
mental_illness_df = extract('SELECT * FROM mentalillness', src_engine)
olympic_hosts_df = extract('SELECT * FROM olympichosts', src_engine)
olympic_medals_df = extract('SELECT * FROM olympicmedals', src_engine)
economic_df = extract('SELECT * FROM economic', src_engine)


# # DimCountry

# In[4]:


# DimCountry(country_name, country_code, region)

# Rename col for consistency
olympic_medals_df_ = olympic_medals_df.rename(columns={'country_code': 'country_code_2', 'country_3_letter_code': 'country_code'})
life_expectancy_df_ = life_expectancy_df.rename(columns={'entity': 'country_name', 'country_3_letter_code': 'country_code'})
economic_df_ = economic_df.rename(columns={'country_3_letter_code': 'country_code'})

combined_df = list_of_countries_df.rename(columns={'country': 'country_name'}).copy()

# Merge with olympic medals for country code
combined_df = pd.merge(combined_df, olympic_medals_df_[['country_name', 'country_code']].drop_duplicates(), on='country_name', how='left')

# Merge + Prioritize non-null values
combined_df = pd.merge(combined_df, life_expectancy_df_[['country_name', 'country_code']].drop_duplicates(), on='country_name', how='left', suffixes=('', '_from_life'))
combined_df['country_code'] = combined_df.apply(lambda row: row['country_code'] if pd.notna(row['country_code']) else row['country_code_from_life'], axis=1)
combined_df.drop(columns='country_code_from_life', inplace=True)
combined_df = pd.merge(combined_df, economic_df_[['country_name', 'country_code']].drop_duplicates(), on='country_name', how='left', suffixes=('', '_from_econ'))
combined_df['country_code'] = combined_df.apply(lambda row: row['country_code'] if pd.notna(row['country_code']) else row['country_code_from_econ'], axis=1)
combined_df.drop(columns='country_code_from_econ', inplace=True)

# No duplicate country_name
combined_df = combined_df.drop_duplicates(subset=['country_name'], keep='first')
combined_df = combined_df[['country_name', 'country_code', 'region']]

# Prioritize country code over NaN
combined_df = combined_df.sort_values(by=['country_name', 'country_code']).drop_duplicates(subset=['country_name'], keep='last')

combined_df.to_sql('dimcountry', trg_engine, if_exists='append', index=False, method='multi')
print("Data successfully loaded into DimCountry.")
combined_df


# # DimTime

# In[5]:


# DimTime (year, game_season)
dim_time_df = olympic_hosts_df.drop_duplicates().rename(columns={'game_year': 'year'})
dim_time_df = dim_time_df[['year', 'game_season']]
dim_time_df.to_sql('dimtime', trg_engine, if_exists='append', index=False)
print("DimTime populated successfully.")
dim_time_df


# # DimAthlete

# In[6]:


# DimAthlete (fullname, gender)
gender_map = {
    'Mixed': 'Mixed',
    'Women': 'Female',
    'Men': 'Male'
}
olympic_medals_df['gender'] = olympic_medals_df['event_gender'].map(gender_map)
dim_athlete_df = olympic_medals_df[['athlete_full_name', 'gender']].drop_duplicates().rename(columns={'athlete_full_name': 'full_name'})
dim_athlete_df = dim_athlete_df.dropna(subset=['gender'])

# Replace NULL with 'Team Event' for 'full_name'
dim_athlete_df.fillna({'full_name': 'Team Event'}, inplace=True)

dim_athlete_df = dim_athlete_df[['full_name', 'gender']]
dim_athlete_df.to_sql('dimathlete', trg_engine, if_exists='append', index=False)
print("DimAthlete populated successfully.")
dim_athlete_df


# # DimEvent

# In[7]:


# DimEvent (event_id, discipline, game_slug)
dim_event_df = olympic_medals_df[['event_title', 'discipline_title', 'slug_game']].drop_duplicates()
dim_event_df = dim_event_df.rename(columns={'discipline_title': 'discipline', 'slug_game': 'game_slug'})
dim_event_df.to_sql('dimevent', con=trg_engine, if_exists='append', index=False)
print("DimEvent populated successfully.")
dim_event_df


# # FactMedalWins

# In[8]:


dim_country_df = extract('SELECT * FROM DimCountry', trg_engine)
dim_athlete_df = extract('SELECT * FROM DimAthlete', trg_engine)
dim_event_df = extract('SELECT * FROM DimEvent', trg_engine)
dim_time_df = extract('SELECT * FROM DimTime', trg_engine)


# In[9]:


country_id_map = dim_country_df.set_index('country_name')['country_id'].to_dict()
athlete_id_map = dim_athlete_df.set_index('full_name')['athlete_id'].to_dict()
event_id_map = dim_event_df.set_index('event_title')['event_id'].to_dict()
time_id_map = dim_time_df.set_index('year')['time_id'].to_dict()


# In[10]:


olympic_medals_df['country_id'] = olympic_medals_df['country_name'].map(country_id_map)
olympic_medals_df.fillna({'athlete_full_name': 'Team Event'}, inplace=True)
olympic_medals_df['athlete_id'] = olympic_medals_df['athlete_full_name'].map(athlete_id_map)
olympic_medals_df['event_id'] = olympic_medals_df['event_title'].map(event_id_map)
olympic_medals_df['year'] = olympic_medals_df['slug_game'].str.extract(r'-(\d{4})$').astype(int)
olympic_medals_df['time_id'] = olympic_medals_df['year'].map(time_id_map)


# In[11]:


fact_medal_wins_df = olympic_medals_df[['country_id', 'athlete_id', 'event_id', 'time_id', 'medal_type']]


# In[12]:


fact_medal_wins_df


# In[13]:


fact_medal_wins_df.to_sql('factmedalwins', trg_engine, if_exists='append', index=False)
print("FactMedalWins populated successfully.")


# # OLAP 
# ## Cube Set Up

# In[32]:


import atoti as tt
session = tt.Session(
    user_content_storage=".content",
    port=9092,
    java_options=["-Xms1G", "-Xmx10G"]
)


# In[33]:


dim_country_df = extract('SELECT * FROM DimCountry', trg_engine)
dim_athlete_df = extract('SELECT * FROM DimAthlete', trg_engine)
dim_event_df = extract('SELECT * FROM DimEvent', trg_engine)
dim_time_df = extract('SELECT * FROM DimTime', trg_engine)
fact_medal_wins_df = extract('SELECT * FROM FactMedalWins', trg_engine)

# Load dimension tables
dim_country_table = session.read_pandas(
    dim_country_df,
    table_name="Country",
    keys=["country_id"]
)
dim_athlete_table = session.read_pandas(
    dim_athlete_df,
    table_name="Athlete",
    keys=["athlete_id"]
)
dim_event_table = session.read_pandas(
    dim_event_df,
    table_name="Event",
    keys=["event_id"]
)
dim_time_table = session.read_pandas(
    dim_time_df,
    table_name="Time",
    keys=["time_id"],
    types={"year": tt.type.INT, "game_season": tt.type.STRING},
    default_values={"year": 0}
)
fact_medal_wins_table = session.read_pandas(
    fact_medal_wins_df,
    table_name="MedalWins",
    keys=["medal_win_id"]
)


# In[34]:


# Join fact table with the dimension tables
fact_medal_wins_table.join(dim_country_table, fact_medal_wins_table["country_id"] == dim_country_table["country_id"])
fact_medal_wins_table.join(dim_athlete_table, fact_medal_wins_table["athlete_id"] == dim_athlete_table["athlete_id"])
fact_medal_wins_table.join(dim_event_table, fact_medal_wins_table["event_id"] == dim_event_table["event_id"])
fact_medal_wins_table.join(dim_time_table, fact_medal_wins_table["time_id"] == dim_time_table["time_id"])

# Create cube
cube = session.create_cube(fact_medal_wins_table)

# Define measures and levels
m = cube.measures
l = cube.levels
h = cube.hierarchies


# In[35]:


session.tables.schema


# In[36]:


h


# # Hierarchies Clean Up

# In[37]:


h["Athlete"] = [l["Athlete", "gender", "gender"], l["Athlete", "full_name", "full_name"]]
h["Country"] = [l["Country", "region", "region"], l["Country", "country_name", "country_name"], l["Country", "country_code", "country_code"]]
h["Event"] = [l["Event", "discipline", "discipline"], l["Event", "game_slug", "game_slug"], l["Event", "event_title", "event_title"]]
h["Time"] = [dim_time_table["game_season"], dim_time_table["year"]]


# In[38]:


del h[('MedalWins', 'medal_type')]
del h[('MedalWins', 'medal_win_id')]


# In[39]:


del h[('Athlete', 'full_name')]
del h[('Athlete', 'gender')]
del h[('Country', 'country_code')]
del h[('Country', 'country_name')]
del h[('Country', 'region')]
del h[('Event', 'event_title')]
del h[('Event', 'discipline')]
del h[('Event', 'game_slug')]
del h[('Time', 'game_season')]


# In[40]:


h


# # Measures Cleanup

# In[41]:


del m["country_id.MEAN"]
del m["country_id.SUM"]
del m["athlete_id.SUM"]
del m["athlete_id.MEAN"]
del m["event_id.SUM"]
del m["event_id.MEAN"]
del m["time_id.SUM"]
del m["time_id.MEAN"]
del m["contributors.COUNT"]


# In[42]:

# Create Measures

m["Total Medals"] = tt.agg.count_distinct(fact_medal_wins_table["medal_win_id"])

# 1. Which country has won the most medals in a specific discipline over all Olympic Games?
m["Top Country by Discipline"] = tt.agg.max_member(
    m["Total Medals"], 
    l["country_name"]
)

# 2. Which discipline has gotten the most medals for a specific country for all Olympic Games?
m["Total Medals by Discipline"] = tt.agg.sum(
    m["Total Medals"],
    scope=tt.OriginScope(l["discipline"], l["region"])
)
m["Top Discipline per Region"] = tt.agg.max_member(
    m["Total Medals by Discipline"], 
    l["discipline"]
)

# 5. How has the performance (in terms of medals won) of a specific country evolved over different Olympic Games?
m["Medals per Game"] = tt.agg.count_distinct(
    fact_medal_wins_table["medal_win_id"]
)


# In[43]:


m


# In[44]:


session.link


# # Business Query

# In[45]:


import pandas as pd

pd.set_option('display.max_rows', None)


# In[46]:


# 1. Which country has won the most medals in a specific discipline over all Olympic Games?
top_country_by_discipline = cube.query(
    m["Total Medals"],
    m["Top Country by Discipline"],
    levels=[l["discipline"]],
    filter=l["discipline"] == "Athletics"
)
top_country_by_discipline.sort_values("Total Medals", ascending=False)


# In[47]:


top_country_by_discipline = cube.query(
    m["Total Medals"],
    m["Top Country by Discipline"],
    levels=[l["discipline"]],
)
top_country_by_discipline.sort_values("Total Medals", ascending=False)


# In[48]:


# 2. Which discipline has gotten the most medals for a specific region for all Olympic Games?
top_discipline_per_region = cube.query(
    m["Top Discipline per Region"],
    m["Total Medals"],
    levels=[l["region"]],
    filter=l["region"] == "Asia"
)
top_discipline_per_region.sort_values("Total Medals", ascending=False)


# In[49]:


top_discipline_per_region = cube.query(
    m["Top Discipline per Region"],
    m["Total Medals"],
    levels=[l["region"]],
)
top_discipline_per_region.sort_values("Total Medals", ascending=False)


# In[50]:


# 3. How does the medal distribution across gender categories compare within a specific discipline over the latest 6 game years?
session.widget


# In[51]:


# 4. How has the performance (in terms of medals won) of a specific country evolved over specific discipline?
session.widget


# In[ ]:





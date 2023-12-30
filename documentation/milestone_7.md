# Milestone 7 - Batch Processing: Spark on Databricks

## Tasks 1-3
### Cleaning the pin, geo and user data in Databricks

Data was cleaned using the code in notebook
(```link to notebook here```) cleaning_batch_data.ipynb

Schema outputs to check data cleaning is shown in notebook:

pin_df:

![pin_schema](screenshots/m7/schema/schema_pin.png)

geo_df:

![geo_schema](screenshots/m7/schema/schema_geo.png)

user_df:

![user_schema](screenshots/m7/schema/schema_user.png)


## Tasks 4-11
### Querying the data

The data was queried using the code in notebook
(```link to notebook here```) querying_batch_data.ipynb

Query outputs are shown in the notebook:

####  Query 1 - Find the most popular category in each country

![query_1](screenshots/m7/queries/1.png)


####  Query 2 - Find which was the most popular category each year between 2018 and 2022

![query_2](screenshots/m7/queries/2.png)


####  Query 3 - Find the user with the most followers in each country

####  Query 3.1 - For each country find the user with the most followers

![query_3.1](screenshots/m7/queries/3.1.png)


####  Query 3.2 - Based on the above query, find the country with the user with most followers

![query_3.2](screenshots/m7/queries/3.2.png)


####  Query 4 - Find the most popular category in each age group

![query_4](screenshots/m7/queries/4.png)


####  Query 5 - Find the median follower count of users in each age group

![query_5](screenshots/m7/queries/5.png)


####  Query 6 - Find how many users have joined each year between 2015 and 2020

![query_6](screenshots/m7/queries/6.png)


####  Query 7 - Find the median follower count of users that joined between 2015 and 2020

![query_7](screenshots/m7/queries/7.png)


####  Query 8 - Find the median follower count of users that joined between 2015 and 2020 in each age group

![query_8](screenshots/m7/queries/8.png)




import findspark
findspark.init()
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


# Initializing spark session
spark = SparkSession \
    .builder \
    .appName(Covid report) \
    .getOrCreate()

#Reading the data
data = spark.read.option('Header', 'True').format('csv').load(<path>)

'''
Data prep 
    - Null to unknown in Province
    - creating date column with filename, as the table has only Last_Update which doesn't provide confirmed cases date
    - Extracting data for the current and past 14th day  
    - Provinces has subgroup based on locality, hence it grouped together to get an aggregate at the province level
'''
#cache technique is used, dataframe is utilized multiple time

df = data\
    .withColumn("Date", to_date(translate(element_at(split(input_file_name(), "/"), -1), '.csv', ''), 'MM-dd-yyyy'))\
    .filter((col('Date').isin((current_date()), (current_date()))))\
    .na.fill("unknown", ['Province_State'])\
    .groupBy(col('Province_State'), col('Country_Region'), col('Date'))\
    .agg(sum(col('Confirmed')).alias('Confirmed'))\
    .cache()

# window to aggregate on a country level and see the increase in confirmed case and ranked based on it

country_diff_window = Window.partitionBy(col('Country_Region')).orderBy(col('Date').desc())
country_agg = df.groupBy(col('Country_Region'),
                         col('Date')
                         .agg(sum(col('Confirmed')).alias('Confirmed')). \
    withColumn('country_increase_count', col('Confirmed') - lead(col('Confirmed')).over(country_diff_window)). \
    where((col('country_increase_count').isNotNull()) & (col('country_increase_count') != 0)). \
    select(rank().over(Window.partitionBy(lit(0)).orderBy(col('country_increase_count'))).alias('country_rank'),
           col('Country_Region'),
           col('country_increase_count')).limit(10)

#Rank for province level to get the top 3 confirmed cases  for top 10 country of decreasing cases

province_window = Window.partitionBy(col('Province_State'), col('df.Country_Region')).orderBy(col('Date').desc())

Final_Df = df.alias('df').join(country_agg.alias('cg'), col('df.Country_Region') == col('cg.Country_Region'), 'INNER'). \
    withColumn('province_increase_count', col('Confirmed') - lead(col('Confirmed')).over(province_window)). \
    where(col('province_increase_count').isNotNull()). \
    select(rank().over(Window.partitionBy(col('df.Country_Region')).orderBy(col('province_increase_count').desc())).alias('province_rank'),
           col('country_rank'), \
           col('df.Country_Region'),
           col('Province_State'),
           col('country_increase_count'),
           col('province_increase_count')).\
    where(col('province_rank') <= 3).sort(col('country_rank'), col('province_rank'))

#writing the report
Final_Df.write.format("Parquet").mode("overwrite").partitionBy('Country_Region').save(<path>)
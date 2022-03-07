# covid-analysis and report

Info
  - Spark Structured streaming is not utilized as it doesn't support multiple aggregation, hence this process will be an event-driven using tool like lambda or with seperate python script
  - The streaming can be implemented for the staging with single aggregation, from the staging it can be made event-driven
  - The Script doesn't follow the EDW process of staging, it is a direct report overwrite with new batches 
  - With the end report it is found for around top 100 country with decreasing cases, the Province_State is not applicable or null, hence the top 3 provinces with cases won't be applicable for top 10 decreasing cases country, but condition can be applied to get the top 10 decreasing cases country with provinces

Data Quality

    - Null in province are replaced with unknown
    - province subgroups are ignored and aggregated on province level (as it out of scope for the requirement)
    - (suggestion) --Country_Region contains non-country entities eg., winter olympics, these can be a data quality issues which can be addressed by maintaining DIM table and performing a quality check using external modules (pydeequ, Great_expectations)

Data prep 

    - Null to unknown in Province
    - creating date column with filename, as the table has only Last_Update which doesn't provide confirmed cases date
    - Extracting data for the current and past 14th day  
    - Provinces has subgroup based on locality, hence it grouped together to get an aggregate at the province level
 

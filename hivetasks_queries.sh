## Creating Table either using direct upload or Sqooping
# hive -e 'CREATE TABLE IF NOT EXISTS project_hive (city string,county string,category_name string,vendor_name string,pack float,bottle_volume_ml float,state_bottle_cost_usd float,state_bottle_retail_usd float,bottles_sold float,sale_usd float,volume_sold_ltr float,volume_sold_gal float) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\054";'

# Sqooping the data into Hive
sqoop import --connect jdbc:mysql://127.0.0.1/Test --username root --password 1324 --table input_table --hive-import --create-hive-table --hive-table project_hive


## Loading data
hive -e 'LOAD DATA LOCAL INPATH "/home/hari/PDA/project_pda/hive_tasks/iowaliqsales_clean.csv" INTO TABLE project_hive;'


## Tasks
#Task 1a
# total of Field 4 values grouped by Field 3 values
hive -e 'select county,sum(bottles_sold) from project_hive group by county;' > /home/hari/PDA/project_pda/hive_tasks/hive_task1a.txt

#Task 1b
# average of Field 4 values grouped by Field 2 values
hive -e 'select county,avg(volume_sold_gal) from project_hive group by county;' > /home/hari/PDA/project_pda/hive_tasks/hive_task1b.txt


#Task 3
# all distinct combinations of Field 2, Field 3, and Field 6 values
hive -e 'select * from project_hive where bottles_sold>0;' > /home/hari/PDA/project_pda/hive_tasks/hive_task3.txt




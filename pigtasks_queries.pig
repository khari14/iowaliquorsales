-- Loading File
liqsales_data = LOAD '/home/hari/PDA/project_pda/sqltable/iowaliqsales_clean.csv' USING PigStorage(',') as (city:chararray,county:chararray,category_name:chararray,vendor_name:chararray,pack:float,bottle_volume_ml:float,state_bottle_cost_usd:float,state_bottle_retail_usd:float,bottles_sold:float,sale_usd:float,volume_sold_ltr:float,volume_sold_gal:float);


-- Task 1
-- total of "pack" values grouped by "county" values
group_county1 = GROUP liqsales_data by county;
sum_packs = FOREACH group_county1 GENERATE group,SUM(liqsales_data.pack);
store sum_packs into '/home/hari/PDA/project_pda/pig_tasks/pig_task1' USING PigStorage(',');

-- Task 2
-- average of "sale_usd" values grouped by "county" values
group_county2 = GROUP liqsales_data by county;
avg_sale_usd = FOREACH group_county2 GENERATE group,AVG(liqsales_data.sale_usd);
store avg_sale_usd into '/home/hari/PDA/project_pda/pig_tasks/pig_task2' USING PigStorage(',');

-- Task 3
-- average of "state_bottle_retail_usd" values grouped by Field 2 values
group_county3 = GROUP liqsales_data by county;
avg_state_bottle_retail_usd = FOREACH group_county3 GENERATE group,AVG(liqsales_data.state_bottle_retail_usd);
store avg_state_bottle_retail_usd into '/home/hari/PDA/project_pda/pig_tasks/pig_task3' USING PigStorage(',');

-- Task 4
-- average of "volume_sold_ltr" values grouped by "county" values
filter_bottles = FILTER liqsales_data by bottles_sold > 1;
group_county4 = GROUP filter_bottles by county;
avg_volume_sold_ltr = FOREACH group_county4 GENERATE group,AVG(filter_bottles.volume_sold_ltr);
store avg_volume_sold_ltr into '/home/hari/PDA/project_pda/pig_tasks/pig_task4' USING PigStorage(',');


-- Task 5
-- "all distinct combinations of city, county, category_name,vendor_name,bottles_sold,volume_sold_gal values"
dist = DISTINCT(FOREACH liqsales_data GENERATE $0, $1, $2, $3, $8, $11);
store dist into '/home/hari/PDA/project_pda/pig_tasks/pig_task5' USING PigStorage(',');




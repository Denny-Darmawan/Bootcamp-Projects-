-- data yang sudah clean --
With Data_clean as
(select consecutive_number, state_name, number_of_vehicle_forms_submitted_all, 
number_of_motor_vehicles_in_transport_mvit, number_of_parked_working_vehicles, 
number_of_forms_submitted_for_persons_not_in_motor_vehicles, number_of_persons_in_motor_vehicles_in_transport_mvit,
number_of_persons_not_in_motor_vehicles_in_transport_mvit, 
case when city_name in ('Not Reported', 'NOT APPLICABLE', 'Other', 'Unknown') then 'Others'
                         else city_name end,
case when land_use_name in ('Not Reported', 'Unknown') then 'Others'
    else land_use_name end,
case when functional_system_name in ('Not Reported', 'Unknown') then 'Others' 
        else functional_system_name end,
case when milepoint in (99998, 99999) then '0'
            else milepoint end,
case when manner_of_collision_name in ('Reported as Unknown', 'Other', 'Not Reported') then 'Others'
                                 else manner_of_collision_name end,
case when type_of_intersection_name in ('Not Reported', 'Reported as Unknown') then 'Others'
                            else type_of_intersection_name end,
case when light_condition_name in ('Reported as Unknown', 'Not Reported', 'Other') then 'Others'
                else light_condition_name end,
case when atmospheric_conditions_1_name in ('Reported as Unknown', 'Other', 'Not Reported') then 'Others'
                    else atmospheric_conditions_1_name end,
number_of_fatalities, number_of_drunk_drivers,
case 
when state_name in ('Oklahoma', 'Alabama', 'Tennessee', 'Kansas', 'Texas', 'Nebraska', 'North Dakota', 'Mississippi', 'Arkansas', 'Missouri', 'Louisiana', 'Illinois', 'Minnesota', 'Iowa', 'Wisconsin', 'South Dakota', 'West Virginia') Then timestamp_of_crash AT time zone 'CST' 
when state_name in('North Carolina', 'Maine', 'Georgia', 'Virginia', 'Maryland', 'New Hampshire', 'Kentucky', 'Massachusetts', 'Pennsylvania', 'Michigan', 'Rhode Island', 'Ohio', 'New Jersey', 'Indiana', 'District of Columbia', 'South Carolina', 'Connecticut', 'New York', 'Florida', 'Vermont', 'Delaware') Then timestamp_of_crash AT time zone 'EST' 
when state_name in('Idaho', 'Wyoming', 'Utah', 'Montana', 'Arizona', 'New Mexico', 'Colorado') Then timestamp_of_crash AT time zone 'MST'
when state_name in('Oregon', 'California', 'Washington', 'Nevada') Then timestamp_of_crash AT time zone 'PST'
when state_name in('Alaska') Then timestamp_of_crash AT time zone 'AKST'
when state_name in ('Hawaii') Then timestamp_of_crash AT time zone 'HST'
end as new_timestamp ,
case 
when state_name in ('Oklahoma', 'Alabama', 'Tennessee', 'Kansas', 'Texas', 'Nebraska', 'North Dakota', 'Mississippi', 'Arkansas', 'Missouri', 'Louisiana', 'Illinois', 'Minnesota', 'Iowa', 'Wisconsin', 'South Dakota', 'West Virginia') Then 'CST' 
when state_name in('North Carolina', 'Maine', 'Georgia', 'Virginia', 'Maryland', 'New Hampshire', 'Kentucky', 'Massachusetts', 'Pennsylvania', 'Michigan', 'Rhode Island', 'Ohio', 'New Jersey', 'Indiana', 'District of Columbia', 'South Carolina', 'Connecticut', 'New York', 'Florida', 'Vermont', 'Delaware') Then 'EST' 
when state_name in('Idaho', 'Wyoming', 'Utah', 'Montana', 'Arizona', 'New Mexico', 'Colorado') Then 'MST'
when state_name in('Oregon', 'California', 'Washington', 'Nevada') Then 'PST'
when state_name in('Alaska') Then 'AKST'
when state_name in ('Hawaii') Then 'HST'
end as timezone_timestamp
from crash),

-- data clean yang di filter hanya tahun 2021 saja --
Data_cleaning as
(select * from Data_clean
where new_timestamp BETWEEN '2021-01-01 00:00:00.000' and '2021-12-31 23:59:59.999'),

-- query type_of_intersection_name --
tin as 
(select 'type_of_intersection_name' as condition_crash,
type_of_intersection_name as remark, count(remark) as count_of_crash,
sum(count(id_count)) over() as total_of_crash,
count(id_count)*1.0 / sum(count(id_count)) over() as persentage_of_crash
from
(select consecutive_number as id_count, state_name, type_of_intersection_name,
number_of_fatalities, number_of_drunk_drivers,
case 
  when type_of_intersection_name = 'Five Point, or More' then 1
  when type_of_intersection_name = 'Four-Way Intersection' then 2
  when type_of_intersection_name = 'L-Intersection' then 3
  when type_of_intersection_name = 'Not an Intersection' then 4
  when type_of_intersection_name = 'Other Intersection Type' then 5
  when type_of_intersection_name = 'Others' then 6
  when type_of_intersection_name = 'Roundabout' then 7
  when type_of_intersection_name = 'T-Intersection' then 8
  when type_of_intersection_name = 'Traffic Circle' then 9
  when type_of_intersection_name = 'Y-Intersection' then 10
  else 0
end as remark
from Data_cleaning
where state_name = 'Texas') as aa
group by 2
order by 3 desc),

-- query land_use_name --
lun as
(select 'land_use_name' as condition_crash,
land_use_name as remark, count(remark) as count_of_crash,
sum(count(id_count)) over() as total_of_crash,
count(id_count)*1.0 / sum(count(id_count)) over() as persentage_of_crash
from
(select consecutive_number as id_count, state_name, land_use_name,
number_of_fatalities, number_of_drunk_drivers,
case 
  when land_use_name = 'Others' then 1
  when land_use_name = 'Rural' then 2
  when land_use_name = 'Trafficway Not in State Inventory' then 3
  when land_use_name = 'Urban' then 4
  else 0
end as remark
from Data_cleaning
where state_name = 'Texas') as aa
group by 2
order by 3 desc),

-- query light_condition_name --
lcn as
(select 'light_condition_name' as condition_crash,
light_condition_name as remark, count(remark) as count_of_crash,
sum(count(id_count)) over() as total_of_crash,
count(id_count)*1.0 / sum(count(id_count)) over() as persentage_of_crash
from
(select consecutive_number as id_count, state_name, light_condition_name,
number_of_fatalities, number_of_drunk_drivers,
case 
  when light_condition_name = 'Dark - Lighted' then 1
  when light_condition_name = 'Dark - Not Lighted' then 2
  when light_condition_name = 'Dark - Unknown Lighting' then 3
  when light_condition_name = 'Dawn' then 4
  when light_condition_name = 'Daylight' then 5
  when light_condition_name = 'Dusk' then 6
  when light_condition_name = 'Others' then 7
  else 0
end as remark
from Data_cleaning
where state_name = 'Texas') as aa
group by 2
order by 3 desc),

-- query atmospheric_conditions_1_name --
acn as
(select 'atmospheric_conditions_1_name' as condition_crash,
atmospheric_conditions_1_name as remark, count(remark) as count_of_crash,
sum(count(id_count)) over() as total_of_crash,
count(id_count)*1.0 / sum(count(id_count)) over() as persentage_of_crash
from
(select consecutive_number as id_count, state_name, atmospheric_conditions_1_name,
number_of_fatalities, number_of_drunk_drivers,
case 
  when atmospheric_conditions_1_name = 'Blowing Sand, Soil, Dirt' then 1
  when atmospheric_conditions_1_name = 'Blowing Snow' then 2
  when atmospheric_conditions_1_name = 'Clear' then 3
  when atmospheric_conditions_1_name = 'Cloudy' then 4
  when atmospheric_conditions_1_name = 'Fog, Smog, Smoke' then 5
  when atmospheric_conditions_1_name = 'Freezing Rain or Drizzle' then 6
  when atmospheric_conditions_1_name = 'Others' then 7
  when atmospheric_conditions_1_name = 'Rain' then 8
  when atmospheric_conditions_1_name = 'Severe Crosswinds' then 9
  when atmospheric_conditions_1_name = 'Sleet or Hail' then 10
  when atmospheric_conditions_1_name = 'Snow' then 11
  else 0
end as remark
from Data_cleaning
where state_name = 'Texas') as aa
group by 2
order by 3 desc)


-- parent query --
select * from tin
union all
select * from lun
union all
select * from lcn
union all
select * from acn
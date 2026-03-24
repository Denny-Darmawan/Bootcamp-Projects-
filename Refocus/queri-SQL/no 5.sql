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
where new_timestamp BETWEEN '2021-01-01 00:00:00.000' and '2021-12-31 23:59:59.999')

-- parent query --
-- Persentasi kecelakaan di daerah pedesaan dan perkotaan --
select 
land_use_name, 
count(consecutive_number) as total,
count(consecutive_number) * 100 / sum(count(consecutive_number)) over () as percentage
from Data_cleaning
where land_use_name in ('Rural','Urban')
group by land_use_name
order by total desc
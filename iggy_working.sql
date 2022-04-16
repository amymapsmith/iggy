select DISTINCT "GEO_ID"
, split_part("GEO_ID",'US',2)::VARCHAR as geoid 
, "B01001_023E" + "B01001_024E" + "B01001_025E" + "B01001_047E" + "B01001_048E" + "B01001_049E" 
	as older_75
, ("B01001_023E" + "B01001_024E" + "B01001_025E" + "B01001_047E" + "B01001_048E" + "B01001_049E" )/
	"B01001_001E"::FLOAT as perc_older
from iggy.sf_age sa 
where 1=1
and "B01001_001E" > 0
limit 100;

select DISTINCT "GEO_ID"
, split_part("GEO_ID",'US',2)::VARCHAR as geoid 
, "C17002_001E" - "C17002_008E" as low_inc
, ("C17002_001E" - "C17002_008E")/"C17002_001E"::FLOAT as perc_low_inc
from iggy.sf_poverty_ratio spr 
where 1=1
and "C17002_001E" > 0;

select DISTINCT "GEO_ID"
, split_part("GEO_ID",'US',2)::VARCHAR as geoid 
, "B25044_003E" + "B25044_010E" as zero_veh
, ("B25044_003E" + "B25044_010E") / "B25044_001E"::FLOAT as perc_zero_veh
from iggy.sf_veh sv 
where 1=1
and "B25044_001E"::FLOAT > 0;

select distinct p."_id" 
, ST_setSRID(p."_geometry", 4326) as geom
, p.cbg 
from ca_sanfrancisco_distances d 
left join ca_sanfrancisco_parcels p 
	on p."_id" = d.id 
left join ca_sanfrancisco_poi poi 
	on poi._source_id  = d."_source_id" 
inner join iggy.sf_feature_layer sfl 
	on sfl.geoid = p.cbg 
	and sfl.priority_area = 1
where 1=1
and (d.source_category = 'is_urgent_health_care' or (d.source_category = 'is_general_health_care' and LOWER(d.source_name) like '%hospital%'))
and poi.cbg <> p.cbg
and LOWER(p.cdl_majority_category) not like '%open space%'
and LOWER(p.cdl_majority_category) not like '%forest%'
and lower(p.zoning_description) not like '%industrial%';

select distinct source_category  from iggy.ca_sanfrancisco_poi csp ;
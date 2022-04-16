create table ca_sanfrancisco_distances as (
	select * from ca_sanfrancisco_distances_0
	union all 
	select * from ca_sanfrancisco_distances_1
	union all 
	select * from ca_sanfrancisco_distances_2
	union all 
	select * from ca_sanfrancisco_distances_3
	union all 
	select * from ca_sanfrancisco_distances_4
	union all 
	select * from ca_sanfrancisco_distances_5
)

alter table ca_sanfrancisco_distances add column geom GEOMETRY;

update ca_sanfrancisco_distances
set geom = ST_GeomFromText(st_astext(source_geometry), 4326);


CREATE INDEX ca_sanfrancisco_distances_geom_idx
  ON ca_sanfrancisco_distances
  USING GIST (geom);
 
CREATE INDEX ca_sanfrancisco_parcels_geom_idx
  ON ca_sanfrancisco_parcels
  USING GIST (_geometry);


alter table public.tl_2021_us_zcta520 add column geom_4326 geometry;
update public.tl_2021_us_zcta520 
set geom_4326 = st_transform(geom,4326);

alter table public.tl_2021_us_zcta520 drop column geom;
alter table public.tl_2021_us_zcta520 rename column geom_4326 to geom;

CREATE INDEX tl_2021_us_zcta520_geom_idx
  ON tl_2021_us_zcta520
  USING GIST (geom);

CREATE INDEX ON ca_sanfrancisco_distances (source_category);

create table iggy.ca_sanfrancisco_poi as (
	select distinct source_category, source_name, _source_id, geom 
	from ca_sanfrancisco_distances csd 
	where 1=1
);

CREATE INDEX ca_sanfrancisco_poi_geom_idx
  ON ca_sanfrancisco_poi
  USING GIST (geom);

alter table ca_sanfrancisco_poi add column zip VARCHAR;
update ca_sanfrancisco_poi as d 
set zip = zcta5ce20
from public.tl_2021_us_zcta520 z
where 1=1 
and ST_CONTAINS(z.geom,d.geom);

alter table ca_sanfrancisco_poi add column cbg VARCHAR;
update ca_sanfrancisco_poi as d 
set cbg = geoid
from public.cbg_california c
where 1=1 
and ST_CONTAINS(c.geom,d.geom);

alter table ca_sanfrancisco_parcels add column zip varchar;
update ca_sanfrancisco_parcels 
set zip = split_part(szip, '-',1); 

alter table ca_sanfrancisco_parcels drop column zip;
alter table ca_sanfrancisco_parcels add column zip varchar;
update ca_sanfrancisco_parcels as p 
set zip = zcta5ce20 
from public.tl_2021_us_zcta520 z
where 1=1 
and ST_CONTAINS(z.geom, st_centroid(st_setsrid(p."_geometry",4326)));

alter table ca_sanfrancisco_parcels add column cbg varchar;
update ca_sanfrancisco_parcels as p 
set cbg = c.geoid 
from public.cbg_california c
where 1=1 
and ST_CONTAINS(c.geom, st_centroid(st_setsrid(p."_geometry",4326)));




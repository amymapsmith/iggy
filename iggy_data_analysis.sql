-- List of Categories
select distinct source_category
from ca_sanfrancisco_distances order by 1;

select source_category
, COUNT(distinct _source_id) 
from ca_sanfrancisco_poi 
where 1=1
--and source_category like '%school%'
--or source_category like '%education%'
--or source_category like '%elementary%'
and source_category like '%grocery%'
group by 1 
order by 2 desc;


---------------------------------------
-- Stop Using Zip Codes
---------------------------------------

-- Percent of locations where nearest POI is outside of zip code

drop table if exists iggy.nearest_poi_outside_zip;
create table iggy.nearest_poi_outside_zip as (
	select d.source_category
	, COUNT(distinct d.id) as parcels
	, AVG(distance_in_meters::FLOAT/1609) as avg_distance_miles
	, COUNT(distinct case when poi.zip <> p.zip then d.id END) as nearest_different_zip
	, (COUNT(distinct case when poi.zip <> p.zip then d.id END)*1.0)/COUNT(distinct d.id) as perc_different_zip
	from ca_sanfrancisco_distances d 
	left join ca_sanfrancisco_parcels p 
		on p."_id" = d.id 
	left join ca_sanfrancisco_poi poi 
		on poi._source_id  = d."_source_id" 
	where 1=1
	and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','6f72da02-276b-4bb9-aa03-296aa39f2bc6','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
	and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
	and p.zip is not null
	and LOWER(p.cdl_majority_category) not like '%open space%'
	and LOWER(p.cdl_majority_category) not like '%forest%'
	and lower(p.zoning_description) not like '%industrial%'
	group by 1 
);

select * from iggy.nearest_poi_outside_zip;

-- Table of areas where closest grocery store is in another zip code

drop table if exists iggy.ca_sanfrancisco_grocery_diff_zip;
create table iggy.ca_sanfrancisco_grocery_diff_zip as (
	select distinct p."_id" 
	, ST_setSRID(p."_geometry", 4326) as geom
	, p.zip 
	from ca_sanfrancisco_distances d 
	left join ca_sanfrancisco_parcels p 
		on p."_id" = d.id 
	left join ca_sanfrancisco_poi poi 
		on poi._source_id  = d."_source_id" 
	where 1=1
	and d.source_category = 'is_grocery_store'
	and poi.zip <> p.zip
	and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
	and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
	and p.zip is not null
);

-- Table of areas where closest school is in another zip code

drop table if exists iggy.ca_sanfrancisco_school_diff_zip;
create table iggy.ca_sanfrancisco_school_diff_zip as (
	select distinct p."_id" 
	, ST_setSRID(p."_geometry", 4326) as geom
	, p.zip 
	from ca_sanfrancisco_distances d 
	left join ca_sanfrancisco_parcels p 
		on p."_id" = d.id 
	left join ca_sanfrancisco_poi poi 
		on poi._source_id  = d."_source_id" 
	where 1=1
	and d.source_category = 'is_elementary_or_secondary_school'
	and poi.zip <> p.zip
	and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','6f72da02-276b-4bb9-aa03-296aa39f2bc6','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
	and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
	and p.zip is not null
	and LOWER(p.cdl_majority_category) not like '%open space%'
	and LOWER(p.cdl_majority_category) not like '%forest%'
	and lower(p.zoning_description) not like '%industrial%'
);

-- Table of areas where closest health care is in another zip code

drop table if exists iggy.ca_sanfrancisco_healthcare_diff_zip;
create table iggy.ca_sanfrancisco_healthcare_diff_zip as (
	select distinct p."_id" 
	, ST_setSRID(p."_geometry", 4326) as geom
	, p.zip 
	from ca_sanfrancisco_distances d 
	left join ca_sanfrancisco_parcels p 
		on p."_id" = d.id 
	left join ca_sanfrancisco_poi poi 
		on poi._source_id  = d."_source_id" 
	where 1=1
	and d.source_category = 'is_health_care'
	and poi.zip <> p.zip
	and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','6f72da02-276b-4bb9-aa03-296aa39f2bc6','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
	and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
	and p.zip is not null
	and LOWER(p.cdl_majority_category) not like '%open space%'
	and LOWER(p.cdl_majority_category) not like '%forest%'
	and lower(p.zoning_description) not like '%industrial%'
);


-- Table of areas where closest rail transportation is in another zip code

drop table if exists iggy.ca_sanfrancisco_railtransport_diff_zip;
create table iggy.ca_sanfrancisco_railtransport_diff_zip as (
	select distinct p."_id" 
	, ST_setSRID(p."_geometry", 4326) as geom
	, p.zip 
	from ca_sanfrancisco_distances d 
	left join ca_sanfrancisco_parcels p 
		on p."_id" = d.id 
	left join ca_sanfrancisco_poi poi 
		on poi._source_id  = d."_source_id" 
	where 1=1
	and d.source_category = 'is_rail_transportation'
	and poi.zip <> p.zip
	and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','6f72da02-276b-4bb9-aa03-296aa39f2bc6','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
	and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
	and p.zip is not null
	and LOWER(p.cdl_majority_category) not like '%open space%'
	and LOWER(p.cdl_majority_category) not like '%forest%'
	and lower(p.zoning_description) not like '%industrial%'
);

---------------------------------------
-- City Deserts
---------------------------------------

-- Grocery

drop table if exists iggy.city_desert_grocery;
create table iggy.city_desert_grocery as (
		select distinct p."_id" 
		, ST_setSRID(p."_geometry", 4326) as geom
		from ca_sanfrancisco_distances d 
		left join ca_sanfrancisco_parcels p 
			on p."_id" = d.id 
		left join ca_sanfrancisco_poi poi 
			on poi._source_id  = d."_source_id" 
		where 1=1
		and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','6f72da02-276b-4bb9-aa03-296aa39f2bc6','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
		and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
		and p.zip is not null
		and LOWER(p.cdl_majority_category) not like '%open space%'
		and LOWER(p.cdl_majority_category) not like '%forest%'
		and LOWER(p.cdl_majority_category) not like '%water%'
		and lower(p.zoning_description) not like '%industrial%'
		and d.source_category = 'is_grocery_store' 
		and (distance_in_meters::FLOAT/1609) > 0.25
);



---------------------------------------
-- Accessibility Hubs
---------------------------------------

drop table if exists iggy.sf_access_hub;
create table iggy.sf_access_hub as (
	with scores as (
		select p."_id" 
		, ST_setSRID(p."_geometry", 4326) as geom
		, MAX(case when d.source_category = 'is_grocery_store' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 END) as grocery 
		, MAX(case when d.source_category = 'is_health_care' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 end) as health_care
		, MAX(case when d.source_category = 'is_convenience_store_or_pharmacy' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 end) as pharmacy
		, MAX(case when d.source_category = 'is_dry_cleaning_and_laundry' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 end) as laundry
		, MAX(case when d.source_category = 'is_gyms_and_fitness_recreation' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 end) as gym
		, MAX(case when d.source_category = 'is_nature_recreation' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 end) as nature
		, MAX(case when d.source_category = 'is_rail_transportation' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 end) as rail
		, MAX(case when d.source_category = 'is_performance_venue' 
			and (distance_in_meters::FLOAT/1609) <= 0.25 then 1 else 0 end) as venue
		from ca_sanfrancisco_distances d 
		left join ca_sanfrancisco_parcels p 
			on p."_id" = d.id 
		left join ca_sanfrancisco_poi poi 
			on poi._source_id  = d."_source_id" 
		where 1=1
		and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','6f72da02-276b-4bb9-aa03-296aa39f2bc6','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
		and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
		and p.zip is not null
		and LOWER(p.cdl_majority_category) not like '%open space%'
		and LOWER(p.cdl_majority_category) not like '%forest%'
		and LOWER(p.cdl_majority_category) not like '%water%'
		and lower(p.zoning_description) not like '%industrial%'
		and d.source_category in ('is_grocery_store','is_health_care' ,'is_convenience_store_or_pharmacy',
			'is_dry_cleaning_and_laundry' , 'is_gyms_and_fitness_recreation' , 'is_nature_recreation',
			'is_rail_transportation' ,'is_performance_venue')
		group by 1,2
	)
	select s.*
	, grocery + health_care + pharmacy + laundry + gym + nature + rail + venue as composite
	from scores s 
	where 1=1 
	and grocery + health_care + pharmacy + laundry + gym + nature + rail + venue >= 3
);


select * from iggy.sf_access_hub;


---------------------------------------
-- Ice Cream
---------------------------------------

drop table if exists iggy.sf_ice_cream;
create table iggy.sf_ice_cream as (
		select distinct p."_id" 
		, ST_setSRID(p."_geometry", 4326) as geom
		from ca_sanfrancisco_distances d 
		left join ca_sanfrancisco_parcels p 
			on p."_id" = d.id 
		left join ca_sanfrancisco_poi poi 
			on poi._source_id  = d."_source_id" 
		where 1=1
		and _id not in ('b4569bad-a863-4b67-ba92-b191c6a0d820','6f72da02-276b-4bb9-aa03-296aa39f2bc6','2b39bd7d-e4f8-4796-bd2d-a4987ab13599','3de81caf-369b-4987-9816-38930eea41b3','8e0e412d-c6f9-49c3-8ca6-62920f4bcbbf','0ed2460a-7de4-4f20-9721-de80b8124e25','d17d49c1-6b22-4d5b-b353-c4436dbfcebe','8dc27817-ea74-49cd-9ede-3d10516582a5','211376cc-5cf4-47f2-a4d7-542495516268','154eb9a0-1323-4ad3-a714-d8e45b207f42','69ab5721-0fdc-4794-b8e2-734de8ae4691','0a853bbb-4aa8-49f1-acee-334f9187e029','8459d82c-a760-4b28-b34e-bd14dbfdc96e','a92ccd05-c181-4339-84fb-e328a63f0ea3','8459d82c-a760-4b28-b34e-bd14dbfdc96e','68364db2-ffcd-42de-a180-f55428052643','ee8dc71d-d468-42a9-a957-3a12d022a87d','39c64d86-a352-4f36-ad50-68d6c4ee3480','d2b28c2f-0121-432a-b269-b0b764465ad4','7d44c112-1970-498e-9357-d005e077bf5f','f03bf87f-635a-4847-a85e-7300451648e6','9e04d6a8-c9f8-48dc-a20e-d6d9f2556d60','7317e702-5a19-4de7-bf34-be0f8b7a37de','84e81d57-c5e4-462d-8513-d34cdf7e5574','206cd2f2-4656-40ff-9431-9d8f9fc054a9','7cd2ae77-9374-4595-8fd0-a372e7f3bae1','cd075217-d203-49d6-8843-8ce5790bc61f','ab1c777c-1616-4806-a603-495e6c5b8119','797ea02e-b4eb-434a-89ee-9bde90e6aad6','f4b9f4ca-6770-4662-bfc8-b1f5cd53481d','0cc0ec0f-5e96-430d-9b6a-a8eb0527e0fd','45c65297-5018-49cc-8fa2-67041c05b166','0c224a47-388b-4fdf-8e9f-387c3749232c','b8201b00-be47-4857-aab5-cdc75bb8ecc1','a4f56206-2301-4502-a31d-9c995d0b10de','8099725a-c1f4-4ae3-a44e-098f10597c50','2553c2a2-eec9-463f-809d-5fda9fd99b46','c1305391-de32-4ff4-a411-882a136e28fd','6e2a9883-f31c-4ef6-8548-c2046a73ee53','5c498283-cb34-461c-a879-b1a5ec5e63d9','fbd90460-c7bc-487e-b8db-17139f35b60c','88452a6c-c247-4963-852b-a0d0e7f2b3c1','3e8dde1a-c2b0-4d38-98de-de60deed81e4','736aa9b7-a3b7-4b8e-b9f0-96f1cff7cb53','f4e836fb-65ff-4a07-a119-b15440b710db','2363dd70-6618-4c4f-a163-9693b5da4eea','ea75fa04-eb88-4afe-a5e0-90615f36126b','2f73d91e-503b-454c-8d27-6f1b807dfab2','90301513-6e5d-4c68-9423-3bcd1421a818','b474b214-0624-43fb-9181-41e3f52441b6','da4d85ae-87a5-4ed4-9760-6faabff1c8ad','000b381c-a4f2-4cd6-a49d-ed8df2e76360','6c665cc4-c20b-419c-82a2-1d032d74e9fb','785e7542-4132-40b4-8001-4db57fabe6ae','2143df83-ada9-4077-af6c-dca968e736a8','854173ed-5559-4296-b140-f627973e62a9','83a5a820-e4d1-4f10-9101-5da73f8b9a61','cf8ba939-18e3-41c9-8123-9c57c5c36b3e','daeb2762-b2e2-4d01-a51b-44be3694258a','9e9c36b5-ba42-4072-9a32-c3038b0abdf3','82f2a8e0-5ba7-4e8c-9875-33669fdab897','91504ef8-4c7d-4662-8c5a-7ea275df90bd','7f0b72d9-c069-41cd-8200-60dde903c77b','79bc6ac8-4a02-4a32-959a-d27c5e993552','9a1309b7-47a8-4a1f-9658-376f54b16a9d')
		and p.zip not in ('94965','94607','94501','94130','',' ','94925','94802','94005','94014','94015')
		and p.zip is not null
		and LOWER(p.cdl_majority_category) not like '%open space%'
		and LOWER(p.cdl_majority_category) not like '%forest%'
		and LOWER(p.cdl_majority_category) not like '%water%'
		and lower(p.zoning_description) not like '%industrial%'
		and LOWER(d.source_name) like '%ice cream%'
		--and (distance_in_meters::FLOAT/1609) <= 1
		and d.source_category in ('is_restaurant','is_business','is_brand')
);



from census import Census
from us import states
import pandas as pd 
from sqlalchemy import create_engine
import folium


engine = create_engine('postgresql://amy@localhost:5432/amy')
c = Census("be588bd861cbf7e0545af372027fc4980855f07e")




Data


Census

# Table B25077: Median Value (Dollars)
# https://censusreporter.org/topics/housing/

housing = pd.DataFrame.from_dict(c.acs5.get(('NAME', 'group(B25077)'),
          {'for': 'tract:*',
           'in': 'state:{} county:001'.format(states.DC.fips)}))



housing.to_sql('dc_median_housing_acs', con=engine)



SNAP


query = """
    CREATE TABLE dc_snap AS (
        SELECT sl.address 
        , sl.address_line__2 
        , sl.city 
        , sl.county 
        , sl.store_name 
        , sl.geom 
        FROM snap_locations sl 
        WHERE 1=1 
        AND state = 'DC'
    );
"""


engine.execute(query)

List of Inputs

bikeshare = 'dc_bikeshare' # https://opendata.dc.gov/datasets/capital-bike-share-locations
bus = 'dc_bus' # https://opendata.dc.gov/datasets/metro-bus-stops
calls = 'dc_311' # https://opendata.dc.gov/datasets/311-city-service-requests-in-2018
crime = 'dc_crime' # https://opendata.dc.gov/datasets/crime-incidents-in-2019
grocery = 'dc_grocery' # https://opendata.dc.gov/datasets/grocery-store-locations
housing = 'dc_median_housing_acs'
liquor = 'dc_liquor' # https://opendata.dc.gov/datasets/liquor-licenses
metro = 'dc_metro' # https://opendata.dc.gov/datasets/54018b7f06b943f2af278bbe415df1de_52
parks = 'dc_parks' # https://opendata.dc.gov/datasets/parks-and-recreation-areas
sidewalks = 'dc_sidewalks' # https://opendata.dc.gov/datasets/sidewalks?geometry=-77.026%2C38.892%2C-77.009%2C38.895
sld = 'dc_sld' # https://www.epa.gov/smartgrowth/smart-location-mapping#SLD
snap = 'dc_snap' # https://www.fns.usda.gov/snap/retailer-locator
trees = 'dc_trees' # https://www.mrlc.gov/viewer/


Compile Dataset

query = """
    DROP TABLE IF EXISTS dc_livability;
    CREATE TABLE dc_livability AS (
        WITH bikeshare AS (
            SELECT geoid10
            , COUNT(DISTINCT terminal_n) AS num_bikeshare_stations
            FROM dc_bikeshare b 
            JOIN dc_sld sld ON ST_Contains(sld.geom, b.geom)
            WHERE 1=1
            GROUP BY 1
        )
        , bus AS (
            SELECT geoid10
            , COUNT(DISTINCT bstp_geo_i) AS num_bus_stations
            FROM dc_bus bus
            JOIN dc_sld sld ON ST_Contains(sld.geom, bus.geom)
            WHERE 1=1
            GROUP BY 1
        )
        , crime AS (
            SELECT geoid10
            , COUNT(DISTINCT ccn) AS num_crimes
            , COUNT(DISTINCT CASE WHEN LOWER(offense) = 'burglary' THEN ccn END) AS num_crimes_burglary
            , COUNT(DISTINCT CASE WHEN LOWER(offense) = 'robbery' THEN ccn END) AS num_crimes_robbery
            , COUNT(DISTINCT CASE WHEN LOWER(offense) LIKE 'assault%%' THEN ccn END) AS num_crimes_assault
            , COUNT(DISTINCT CASE WHEN LOWER(offense) = 'homicide' THEN ccn END) AS num_crimes_homicide
            , COUNT(DISTINCT CASE WHEN LOWER(offense) LIKE '%%theft%%' THEN ccn END) AS num_crimes_theft
            , COUNT(DISTINCT CASE WHEN LOWER(offense) = 'arson' THEN ccn END) AS num_crimes_arson
            , COUNT(DISTINCT CASE WHEN LOWER(offense) = 'sex_abuse' THEN ccn END) AS num_crimes_sex_abuse
            FROM dc_crime c 
            JOIN dc_sld sld ON ST_Contains(sld.geom, c.geom)
            WHERE 1=1 
            GROUP BY 1
        )
        , grocery AS (
            SELECT geoid10
            , COUNT(DISTINCT gis_id) AS num_grocery_stores
            FROM dc_grocery g 
            JOIN dc_sld sld ON ST_Contains(sld.geom, g.geom)
            WHERE 1=1
            AND present18 = 'Yes'
            GROUP BY 1
        )
        , housing AS (
            SELECT DISTINCT geoid10
            , "B25077_001E" AS median_home_value
            FROM dc_sld sld
            JOIN dc_median_housing_acs h ON h.tract = sld.trfips
            WHERE 1=1
            AND "B25077_001E" > 0
        )
        , liquor AS (
            SELECT DISTINCT geoid10
            , COUNT(DISTINCT license) AS num_liquor_licenses
            , COUNT(DISTINCT CASE WHEN type IN ('Club','Night Club','Tavern','Restaurant') THEN license END) AS num_liquor_licenses_bar_restaurant
            FROM dc_liquor l 
            JOIN dc_sld sld ON ST_Contains(sld.geom, l.geom)
            WHERE 1=1
            AND status = 'Active'
            GROUP BY 1
        )
        , metro AS (
            SELECT geoid10
            , COUNT(DISTINCT gis_id) AS num_metro_stations
            FROM dc_metro m
            JOIN dc_sld sld ON ST_Contains(sld.geom, m.geom)
            WHERE 1=1
            GROUP BY 1
        )
        , parks AS (
            SELECT geoid10
            , SUM(p.shape_area) AS park_area
            FROM dc_parks p
            JOIN dc_sld sld ON ST_Intersects(sld.geom, p.geom)
            WHERE 1=1
            GROUP BY 1
        )
        , sidewalks AS (
            SELECT geoid10
            , SUM(s.shape_area) AS sidewalk_area
            FROM dc_sidewalks s 
            JOIN dc_sld sld ON ST_Intersects(sld.geom, s.geom)
            WHERE 1=1 
            GROUP BY 1
        )
        , snap AS (
            SELECT geoid10
            , COUNT(DISTINCT address || store_name) AS num_snap_retail
            FROM dc_snap sn
            JOIN dc_sld sld ON ST_Contains(sld.geom, sn.geom)
            WHERE 1=1
            GROUP BY 1
        )
        , trees AS (
            SELECT geoid10
            , AVG(dn) AS avg_tree_density
            , COUNT(DISTINCT CASE WHEN dn > 0 THEN tr.id END)*30 AS tree_cover_area
            FROM dc_trees tr
            JOIN dc_sld sld ON ST_Intersects(sld.geom, tr.geom)
            WHERE 1=1
            GROUP BY 1
        )
        
        SELECT sld.geoid10
        , sld.geom
        , sld.ac_land AS land_area
        , sld.d1b AS pop_density
        , sld.d2a_ephhm AS job_pop_mix
        
        -- Transportation
        , COALESCE(num_bikeshare_stations,0) AS num_bikeshare_stations
        , COALESCE(num_bus_stations, 0) AS num_bus_stations
        , COALESCE(num_metro_stations, 0) AS num_metro_stations
        , d4d AS transit_freq
        
        -- Safety
        , COALESCE(num_crimes, 0) AS num_crimes
        , COALESCE(num_crimes_burglary, 0) AS num_crimes_burglary
        , COALESCE(num_crimes_robbery, 0) AS num_crimes_robbery
        , COALESCE(num_crimes_assault, 0) AS num_crimes_assault
        , COALESCE(num_crimes_homicide, 0) AS num_crimes_homicide
        , COALESCE(num_crimes_theft, 0) AS num_crimes_theft
        , COALESCE(num_crimes_arson, 0) AS num_crimes_arson
        , COALESCE(num_crimes_sex_abuse, 0) AS num_crimes_sex_abuse
        
        -- Health & Environment
        , COALESCE(park_area, 0) AS park_area
        , COALESCE(sidewalk_area, 0) AS sidewalk_area
        , COALESCE(tree_cover_area, 0) AS tree_cover_area
        , COALESCE(avg_tree_density, 0) AS avg_tree_density
        
        -- Affordability
        , median_home_value
        , COALESCE(num_snap_retail,0) AS num_snap_retail
        
        -- Amenities
        , COALESCE(num_grocery_stores,0) AS num_grocery_stores
        , COALESCE(num_liquor_licenses,0) AS num_liquor_licenses
        , COALESCE(num_liquor_licenses_bar_restaurant,0) AS num_liquor_licenses_bar_restaurant
        
        -- Access
        , d5ar jobs_45_min_drive
        , d5br jobs_45_min_transit
        
        FROM dc_sld sld 
        LEFT JOIN bikeshare bike ON bike.geoid10 = sld.geoid10
        LEFT JOIN bus ON bus.geoid10 = sld.geoid10
        LEFT JOIN crime c ON c.geoid10 = sld.geoid10
        LEFT JOIN grocery g ON g.geoid10 = sld.geoid10
        LEFT JOIN housing h ON h.geoid10 = sld.geoid10
        LEFT JOIN liquor l ON l.geoid10 = sld.geoid10
        LEFT JOIN metro m ON m.geoid10 = sld.geoid10
        LEFT JOIN parks p ON p.geoid10 = sld.geoid10
        LEFT JOIN sidewalks s ON s.geoid10 = sld.geoid10
        LEFT JOIN snap sn ON sn.geoid10 = sld.geoid10
        LEFT JOIN trees t ON t.geoid10 = sld.geoid10
        
        WHERE 1=1
    );
"""


engine.execute(query)

# Individual Scores


# Transportation

# Add Density Columns

query = """
    ALTER TABLE dc_livability ADD COLUMN bikeshare_density FLOAT;
    ALTER TABLE dc_livability ADD COLUMN bus_density FLOAT;
    ALTER TABLE dc_livability ADD COLUMN metro_density FLOAT;
    UPDATE dc_livability 
    SET bikeshare_density = num_bikeshare_stations::FLOAT/land_area
    , bus_density = num_bus_stations::FLOAT/land_area
    , metro_density = num_metro_stations::FLOAT/land_area
    ; 
"""

engine.execute(query)

# Add Percentiles

query = """
    ALTER TABLE dc_livability ADD COLUMN bikeshare_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN bus_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN metro_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN transit_freq_ntile FLOAT;
    WITH percentiles AS (
        SELECT geoid10
        , PERCENT_RANK() OVER (ORDER BY bikeshare_density) AS bikeshare_ntile
        , PERCENT_RANK() OVER (ORDER BY bus_density) AS bus_ntile
        , PERCENT_RANK() OVER (ORDER BY metro_density) AS metro_ntile
        , PERCENT_RANK() OVER (ORDER BY transit_freq) AS transit_freq_ntile
        FROM dc_livability
        WHERE 1=1
    )
    UPDATE dc_livability AS l
    SET bikeshare_ntile = p.bikeshare_ntile
    , bus_ntile = p.bus_ntile
    , metro_ntile = p.metro_ntile
    , transit_freq_ntile = p.transit_freq_ntile
    FROM percentiles p 
    WHERE 1=1
    AND p.geoid10 = l.geoid10
    ;
"""

engine.execute(query)

# Score

query = """
    ALTER TABLE dc_livability ADD COLUMN transit_score FLOAT;
    UPDATE dc_livability 
    SET transit_score = (bikeshare_ntile + bus_ntile + metro_ntile + transit_freq_ntile)/4
    ;
"""

engine.execute(query)




# Safety

# Add Density Column

query = """
    ALTER TABLE dc_livability ADD COLUMN crime_density FLOAT;
    UPDATE dc_livability 
    SET crime_density = num_crimes::FLOAT/land_area
    ; 
"""

engine.execute(query)

# Add Score

query = """
    ALTER TABLE dc_livability ADD COLUMN crime_score FLOAT;
    WITH percentiles AS (
        SELECT geoid10
        , PERCENT_RANK() OVER (ORDER BY crime_density DESC) AS crime_score
        FROM dc_livability
        WHERE 1=1
    )
    UPDATE dc_livability AS l
    SET crime_score = p.crime_score
    FROM percentiles p 
    WHERE 1=1
    AND p.geoid10 = l.geoid10
    ;
"""

engine.execute(query)


# Health & Environment

# Add Density Columns

query = """
    ALTER TABLE dc_livability ADD COLUMN tree_area_density FLOAT;
    UPDATE dc_livability 
    SET tree_area_density = tree_cover_area::FLOAT/land_area
    ; 
"""

engine.execute(query)

# Add Percentiles

query = """
    ALTER TABLE dc_livability ADD COLUMN park_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN sidewalk_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN tree_area_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN tree_density_ntile FLOAT;
    WITH percentiles AS (
        SELECT geoid10
        , PERCENT_RANK() OVER (ORDER BY park_area) AS park_ntile
        , PERCENT_RANK() OVER (ORDER BY sidewalk_area) AS sidewalk_ntile
        , PERCENT_RANK() OVER (ORDER BY tree_area_density) AS tree_area_ntile
        , PERCENT_RANK() OVER (ORDER BY avg_tree_density) AS tree_density_ntile
        FROM dc_livability
        WHERE 1=1
    )
    UPDATE dc_livability AS l
    SET park_ntile = p.park_ntile
    , sidewalk_ntile = p.sidewalk_ntile
    , tree_area_ntile = p.tree_area_ntile
    , tree_density_ntile = p.tree_density_ntile
    FROM percentiles p 
    WHERE 1=1
    AND p.geoid10 = l.geoid10
    ;
"""

engine.execute(query)

# Score

query = """
    ALTER TABLE dc_livability DROP COLUMN env_score;
    ALTER TABLE dc_livability ADD COLUMN env_score FLOAT;
    UPDATE dc_livability 
    SET env_score = (sidewalk_ntile + tree_area_ntile + tree_density_ntile)/3
    ;
"""

engine.execute(query)

# AFFORDABILITY

# Add Percentiles

query = """
    ALTER TABLE dc_livability ADD COLUMN home_value_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN snap_ntile FLOAT;
    WITH percentiles AS (
        SELECT geoid10
        , PERCENT_RANK() OVER (ORDER BY median_home_value DESC) AS home_value_ntile
        , PERCENT_RANK() OVER (ORDER BY num_snap_retail) AS snap_ntile
        FROM dc_livability
        WHERE 1=1
    )
    UPDATE dc_livability AS l
    SET home_value_ntile = p.home_value_ntile
    , snap_ntile = p.snap_ntile
    FROM percentiles p 
    WHERE 1=1
    AND p.geoid10 = l.geoid10
    ;
"""

engine.execute(query)

# Score

query = """
    ALTER TABLE dc_livability ADD COLUMN aff_score FLOAT;
    UPDATE dc_livability 
    SET aff_score = (home_value_ntile + snap_ntile)/2
    ;
"""

engine.execute(query)

# AMENITIES

# Add Density Columns

query = """
    ALTER TABLE dc_livability ADD COLUMN grocery_density FLOAT;
    ALTER TABLE dc_livability ADD COLUMN liquor_density FLOAT;
    UPDATE dc_livability 
    SET grocery_density = num_grocery_stores::FLOAT/land_area
    , liquor_density = num_liquor_licenses_bar_restaurant::FLOAT/land_area
    ; 
"""

engine.execute(query)

# Add Percentiles

query = """
    ALTER TABLE dc_livability ADD COLUMN grocery_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN liquor_ntile FLOAT;
    WITH percentiles AS (
        SELECT geoid10
        , PERCENT_RANK() OVER (ORDER BY grocery_density) AS grocery_ntile
        , PERCENT_RANK() OVER (ORDER BY liquor_density) AS liquor_ntile
        FROM dc_livability
        WHERE 1=1
    )
    UPDATE dc_livability AS l
    SET grocery_ntile = p.grocery_ntile
    , liquor_ntile = p.liquor_ntile
    FROM percentiles p 
    WHERE 1=1
    AND p.geoid10 = l.geoid10
    ;
"""

engine.execute(query)

# Score

query = """
    ALTER TABLE dc_livability ADD COLUMN amenity_score FLOAT;
    UPDATE dc_livability 
    SET amenity_score = (grocery_ntile + liquor_ntile)/2
    ;
"""

engine.execute(query)

# ACCESS

# Add Percentiles

query = """
    ALTER TABLE dc_livability ADD COLUMN jobs_drive_ntile FLOAT;
    ALTER TABLE dc_livability ADD COLUMN jobs_transit_ntile FLOAT;
    WITH percentiles AS (
        SELECT geoid10
        , PERCENT_RANK() OVER (ORDER BY jobs_45_min_drive) AS jobs_drive_ntile
        , PERCENT_RANK() OVER (ORDER BY jobs_45_min_transit) AS jobs_transit_ntile
        FROM dc_livability
        WHERE 1=1
    )
    UPDATE dc_livability AS l
    SET jobs_drive_ntile = p.jobs_drive_ntile
    , jobs_transit_ntile = p.jobs_transit_ntile
    FROM percentiles p 
    WHERE 1=1
    AND p.geoid10 = l.geoid10
    ;
"""

engine.execute(query)

# Score

query = """
    ALTER TABLE dc_livability ADD COLUMN access_score FLOAT;
    UPDATE dc_livability 
    SET access_score = (jobs_drive_ntile + jobs_transit_ntile)/2
    ;
"""

engine.execute(query)

# Calculate Index

query = """
    ALTER TABLE dc_livability DROP COLUMN livability_score;
    ALTER TABLE dc_livability ADD COLUMN livability_score FLOAT;
    UPDATE dc_livability
    SET livability_score = (transit_score + crime_score + env_score + aff_score + amenity_score + access_score)::FLOAT/6
    ;
"""

engine.execute(query)




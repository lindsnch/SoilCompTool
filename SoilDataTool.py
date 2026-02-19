import sys
import geopandas as gpd
import requests as req
import os
import shapely

gdb_path = input("Enter the full path to the geodatabase: ")
feature_class = input("Enter the feature class name within the geodatabase: ")
if not os.path.exists(gdb_path):
    print("The specified GDB path does not exist.")
    sys.exit(1)

aoi = gpd.read_file(filename=gdb_path, layer=feature_class)
aoi = aoi.to_crs(epsg=4326)
aoi_wkt = aoi.geometry.union_all().wkt

# create AOI on SDA server
aoi_request = {
    "SERVICE": "aoi",
    "REQUEST": "create",
    "AOICOORDS": aoi_wkt
}

soils_url = "https://sdmdataaccess.nrcs.usda.gov/tabular/post.rest"
response = req.post(soils_url, data=aoi_request)

print("success")

aoi_id = response.text
print("AOI ID:", aoi_id)

pAOI = response.json()["id"]
print("pAOI: ", pAOI)

import pandas as pd

# extract mukeys within the AOI
key_request = f"""
SELECT *
FROM SDA_Get_AoiMapunit_By_AoiID({pAOI})
"""

response = req.post(soils_url, data={'query': key_request, 'format': 'JSON'})
data = response.json()["Table"]
df = pd.DataFrame(data)
df.columns = ["aoiid", "aoikey", "mukey"]
mukeys = df["mukey"].unique().tolist()
print("Mukeys in AOI:", mukeys)

# provides the option to view the attribute catalog for the defined AOI
# likely won't need to include in final script but leaving it here for my own use
check_catalog = input("Would you like to see the attribute catalog for this AOI? (yes/no): ")
while True:
    if check_catalog.lower() == 'yes':
        catalog_query = {
            "SERVICE": "interpretation",
            "REQUEST": "getcatalog",
            "AOIID": pAOI
        }
        response = req.post(soils_url, data=catalog_query)
        print(response.text)
        break
    else:
        print("Skipping catalog display.")
        break

# table containing all available soils properties/ratings
available_ratings = {
    "clay":{
        "table": "chorizon",
        "column": "claytotal_r",
        "units": "percent"
    },
    "sand":{
        "table": "chorizon",
        "column": "sandtotal_r", 
        "units": "percent"
    }, 
    "silt":{
        "table": "chorizon",
        "column": "silttotal_r",
        "units": "percent"
    },
    "ksat":{
        "table": "chorizon",
        "column": "ksat_r",
        "units": "cm/hr"
    }
}

# all further ratings will be pulled based on the corresponding mukeys, from the mukey list created in the previous step
# prompt user to enter and confirm ratings to pull
print("Available ratings/properties to pull include: clay, sand, silt, ksat")
ratings = input("Enter properties to pull ratings for: ")
print("Ratings to pull:", ratings)
check_ratings = input("Is this correct? (yes/no): ")
while True:
    if check_ratings.lower() == 'yes':
        break
    else:
        ratings = input("Re-enter ratings to pull for the defined AOI: ")
        print("Ratings to pull:", ratings)
        check_ratings = input("Is this correct? (yes/no): ")
        continue
while True:
    if ratings in available_ratings:
            print("Proceeding with ratings:", ratings)
            break
    else:
            print("Invalid property/properties entered. Please enter a valid property from the available ratings.")
            ratings = input("Enter properties to pull ratings for: ")
            print("Ratings to pull:", ratings)
            check_ratings = input("Is this correct? (yes/no): ")
    while True:
        if check_ratings.lower() == 'yes':
            print("Proceeding with ratings:", ratings)
            break
        else:
            ratings = input("Re-enter ratings to pull for the defined AOI: ")
            print("Ratings to pull:", ratings)
            check_ratings = input("Is this correct? (yes/no): ")
            continue
    continue
top_depth = input("Enter desired top depth in cm: ")
bottom_depth = input("Enter desired bottom depth in cm: ")

def depth_query(ratings, top_depth, bottom_depth):
    rating_column = available_ratings[ratings]['column']
    return f"""
    SUM(
        CASE
            WHEN ch.hzdepb_r <= {top_depth} THEN 0
            WHEN ch.hzdept_r >= {bottom_depth} THEN 0
            WHEN ch.hzdepb_r > {bottom_depth} THEN ({bottom_depth} - ch.hzdept_r)
            ELSE (ch.hzdepb_r - ch.hzdept_r)
        END
        * ch.{rating_column}
        * c.comppct_r
    )
    /
    SUM(
        CASE
            WHEN ch.hzdepb_r <= {top_depth} THEN 0
            WHEN ch.hzdept_r >= {bottom_depth} THEN 0
            WHEN ch.hzdepb_r > {bottom_depth} THEN ({bottom_depth} - ch.hzdept_r)
            ELSE (ch.hzdepb_r - ch.hzdept_r)
        END
        * c.comppct_r
    ) 
"""
rating_column = available_ratings[ratings]['column']
ratings_query = f"""
SELECT 
    mu.mukey,
    {depth_query(ratings, top_depth, bottom_depth)} AS {ratings}_{top_depth}_{bottom_depth}cm
FROM mapunit mu
JOIN component c ON mu.mukey = c.mukey
JOIN chorizon ch ON c.cokey = ch.cokey
WHERE mu.mukey IN ({', '.join([str(mukey) for mukey in mukeys])}) 
GROUP BY mu.mukey
"""

response = req.post(soils_url, data={'query': ratings_query, 'format': 'JSON'})
ratings_df = pd.DataFrame(response.json()["Table"])
ratings_df.columns = ["mukey", f"{ratings}_{top_depth}_{bottom_depth}cm"]
#convert ratings to numeric values
ratings_df[f"{ratings}_{top_depth}_{bottom_depth}cm"] = pd.to_numeric(ratings_df[f"{ratings}_{top_depth}_{bottom_depth}cm"], errors='coerce')
print(ratings_df)

# spatial data in WGS84, EPSG:4326    
spatial_request = f"""
SELECT *
FROM SDA_Get_AoiSoilMapunitPolygon_By_AoiId({pAOI})
"""

response = req.post(soils_url, data={'query': spatial_request, 'format': 'JSON'})
print("Status code:", response.status_code)
print("Raw response:", response.text)
spatial_df = pd.DataFrame(response.json()["Table"])

spatial_df.columns = ["1", "2", "3", "geom", "5", "6", "7", "mukey", "9", "10", "11", "12", "13", "14"]
spatial_df = spatial_df[["mukey", "geom"]]

merged_df = pd.merge(spatial_df, ratings_df, on="mukey", how="outer")
print(merged_df)

from shapely import wkt
merged_df["geom"] = merged_df["geom"].apply(wkt.loads)
spatial_gdf = gpd.GeoDataFrame(merged_df, geometry="geom", crs="EPSG:4326")

dir = os.path.dirname(gdb_path)
filename = input("Enter desired filename for results shapefile (without extension): ")
shape_path = os.path.join(dir, f"{filename}.shp")
spatial_gdf.to_file(shape_path)
print(f"AOI shapes saved to: {shape_path}")
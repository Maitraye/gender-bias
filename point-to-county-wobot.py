import pandas as pd
import numpy as np
import math
import json
import csv

from shapely.wkt import loads
from shapely.geometry import shape
from shapely.geometry import box

counties_fn = 'USCounties_bare.geojson'
with open(counties_fn, 'r') as fin:
    counties_gj = json.load(fin)

states_fn = 'US_States_from_counties.geojson'
with open(states_fn, 'r') as fin:
    states_gj = json.load(fin)

counties = {}
for state in states_gj['features']:
    west, south, east, north = shape(state['geometry']).bounds
    counties[state['properties']['FIPS'][:2]] = {'bb':box(west, south, east, north), 'counties':{}}
for region in counties_gj['features']:
    state_fips = region['properties']['FIPS'][:2]
    counties[state_fips]['counties'][region['properties']['FIPS']] = shape(region['geometry'])
del(counties_gj)
del(states_gj)

eastUS = -66.9513812
westUS = -124.7844079
northUS = 49.3457868
southUS = 24.7433195
boundingboxUS = box(westUS, southUS, eastUS, northUS)

def point_to_county (index, group):
    filename = "node-counties" + str(index)
    OUTPUT_FN = '/home/mdg9047/node-counties/' + filename + '.csv'
    OUTPUT_HEADER = ['nid', 'county', 'gender']

    fast_lookup = {}
    total_points = 0
    points_in_US = 0
    count_lines = 0
    with open(OUTPUT_FN, 'w') as fout:
        csvwriter = csv.writer(fout)
        csvwriter.writerow(OUTPUT_HEADER)
        
        for row in group[index].itertuples(index=True, name='Pandas'):
            count_lines += 1
            county = None
            nid = getattr(row, "nid")
            lat = getattr(row, "lat")
            lon = getattr(row, "lon")
            gender = getattr(row, "gender")
            latlon = ','.join([lat, lon])
            
            try:
                pt = loads('POINT ({0} {1})'.format(lon.strip(), lat.strip()))
                total_points += 1
                if latlon in fast_lookup:
                    county = fast_lookup[latlon]
                    points_in_US += 1
                else:
                    if boundingboxUS.contains(pt):
                        for state in counties:
                            if counties[state]['bb'].contains(pt):
                                for fips in counties[state]['counties']:
                                    if counties[state]['counties'][fips].contains(pt):
                                        county = fips
                                        points_in_US += 1
                                        fast_lookup[latlon] = county
                                        break
                    else:
                    	pass
                        # print(boundingboxUS.contains(pt))
            except Exception as e:
                if getattr(row, "nid"):
                    print(e)
                    print(getattr(row, "nid"))
            csvwriter.writerow([nid, county, gender])

def convert_group (df, index):
    df[index]['nid'] = df[index]['nid'].astype(str)
    df[index]['lat'] = df[index]['lat'].astype(str)
    df[index]['lon'] = df[index]['lon'].astype(str)
    print(df[index].info())

def county_concat(c1, c2):
    return pd.concat([c1, c2], ignore_index=True)

usa_node_gender_wobot = pd.read_csv("/home/mdg9047/usa_node_gender_wobot.csv")
print(usa_node_gender_wobot.info())

const = 2000000
div = math.ceil (usa_node_gender_wobot.shape[0] / const)
print("USA wobot is divided into " + str(div))
group = np.array_split(usa_node_gender_wobot, div)
for i in range(0, div):
    convert_group(group, i)
    point_to_county (i, group)


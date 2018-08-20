import pandas as pd
import numpy as np
import math
import json
import csv

from shapely.wkt import loads
from shapely.geometry import shape
from shapely.geometry import box

user_dataset = pd.read_csv("/home/mdg9047/user_final.csv")

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

def point_to_county (state, index, group):
    filename = state + str(index)
    OUTPUT_FN = '/home/mdg9047/node-counties-with-bot/' + filename + '.csv'
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
#                         print(boundingboxUS.contains(pt))
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

def drop_column(df):
    df.drop(['Unnamed: 0', 'timestamp', 'uid', 'changeset'], axis = 1, inplace = True)
    print(df.info())

def drop_column_cal(df):
    df.drop(['Unnamed: 0'], axis = 1, inplace = True)

def state_concat(s1, s2):
    return pd.concat([s1,s2], ignore_index = True)


states = 	['alabama', 'arizona', 'arkansas', 'colorado', 'connecticut', 'dc', 'delaware', 'florida', 'georgia', 'idaho', 'illinois',
          	'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusettes', 'michigan', 'minnesota',
          	'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new-hampshire', 'new-jersey', 'new-mexico', 'new-york', 
          	'north-carolina', 'north-dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode-island', 'south-carolina', 
          	'south-dakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 'west-virginia', 'wisconsin', 'wyoming']

const = 2000000

for state in states:
	if state is 'california':
		state_df = pd.read_csv("/home/mdg9047/node-with-bot/california-node1.csv")
		drop_column_cal(state_df)
		state_df = pd.merge(state_df, user_dataset, how = "inner", on = "user")

		for i in range (2, 11):
			fname = "/home/mdg9047/node-with-bot/california-node" + str(i) + ".csv"
    		ca_df = pd.read_csv(fname)
    		drop_column_cal(ca_df)

    		ca_df = pd.merge(ca_df, user_dataset, how = "inner", on = "user")
		    
		    state_df = state_concat(state_df, ca_df)

		state_df.drop_duplicates(inplace = True)
		state_df.reset_index(drop = True, inplace = True) 
		state_df.drop(['timestamp', 'uid', 'changeset', 'user'], axis = 1, inplace = True)
		print(state_df.info())

	else:
    	fname = "/home/mdg9047/node-with-bot/" + state + "-node.csv"
    
    	state_df = pd.read_csv(fname)
    	drop_column (state_df)
    
	    state_df = pd.merge(state_df, user_dataset, how = "inner", on = "user")
	    print(state + " info after merging with user db: ")
	    print(state_df.info())
    
    # outfname = "/home/mdg9047/node-gender-with-bot_12k/" + state + ".csv"
    # state_df.to_csv(outfname, index = False)
    
    div = math.ceil (state_df.shape[0] / const)
    print(state + " is divided into " + str(div))
    state_group = np.array_split(state_df, div)
    for i in range(0, div):
        convert_group(state_group, i)
        point_to_county (state, i, state_group)

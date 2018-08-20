import pandas as pd

def state_concat(s1, s2):
    return pd.concat([s1,s2], ignore_index = True)

usa = pd.DataFrame()

states = {'alabama':4,
          'arizona':5,
          'arkansas':2,
          'california':31,
          'colorado':6,
          'connecticut':0,
          'dc':0,
          'delaware':0,
          'florida':7,
          'georgia':12,
          'idaho':3,
          'illinois':3,
          'indiana':2,
          'iowa':3,
          'kansas':2,
          'kentucky':4,
          'louisiana':8,
          'maine':2,
          'maryland':8,
          'massachusettes':8,
          'michigan':8,
          'minnesota':8,
          'mississippi':3,
          'missouri':4,
          'montana':2,
          'nebraska':3,
          'nevada':3,
          'new-hampshire':3,
          'new-jersey':8,
          'new-mexico':3,
          'new-york':8,
          'north-carolina':8,
          'north-dakota':5,
          'ohio':8,
          'oklahoma':3,
          'oregon':8,
          'pennsylvania':10,
          'rhode-island':1,
          'south-carolina':8,
          'south-dakota':3,
          'tennessee':8,
          'texas':10,
          'utah':8,
          'vermont':1,
          'virginia':8,
          'washington':8,
          'west-virginia':2,
          'wisconsin':5,
          'wyoming':3
         }
for state in states.keys():
    if states[state] == 0:
        fname = "/home/mdg9047/node-counties-with-bot/" + state + ".csv"
        state_df = pd.read_csv(fname)
        print(state)
        print(state_df.info())
        state_df.dropna(inplace = True)
        state_df.reset_index(drop = True, inplace = True)
        print(state_df.info())
    
        usa = state_concat(usa, state_df)
        print("usa after adding " + state)
        print(usa.info())
    else:
        for i in range(0,states[state]):
            fname = "/home/mdg9047/node-counties-with-bot/" + state + str(i) + ".csv"
            state_df = pd.read_csv(fname)
            print(state + str(i))
            print(state_df.info())
            state_df.dropna(inplace = True)
            state_df.reset_index(drop = True, inplace = True)
            print(state_df.info())

            usa = state_concat(usa, state_df)
            print("usa after adding " + state + str(i))
            print(usa.info())


usa.to_csv("/home/mdg9047/node-counties-with-bot/usa_wbot.csv")

county_gender = usa.groupby(['county', 'gender'], as_index = False)[['nid']].count()
county_gender_spread = county_gender.pivot(index='county',columns='gender',values='nid')
county_gender_spread.reset_index(level=0, inplace=True)
county_gender_spread.fillna(0, inplace = True)
county_gender_spread['county'] = county_gender_spread['county'].astype(str)
county_gender_spread.to_csv("/home/mdg9047/node-counties-with-bot/county_gender.csv", index = False)
check = pd.read_csv("/home/mdg9047/node-counties-with-bot/county_gender.csv")
print(check.info())
print(check.isnull().sum())
print(check.sample(5))s
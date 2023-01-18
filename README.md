```python
from collections import Counter
import pandas as pd
import numpy as np
import sqlalchemy as sa

# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_rows', df_Nest.shape[0]+1)
# pd.set_option('display.min_rows', 100)

```

Destination dataframes:
1. איתור והעתקה
2. הפקדה
3. ביקורת תטולה
4. פתיחת קן



```python
## import data

driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
db_path = "C:/Users/michalsh/Downloads/TurtlesDB_be.mdb"
engine_string = 'DRIVER=%s;DBQ=%s;' % (driver,db_path)
engine_url = sa.engine.URL.create(
    "access+pyodbc",
    query={"odbc_connect": engine_string})

engine = sa.create_engine(engine_url)

table_names=['AcHatch','AcCrawl', 'Activities', 'Clutches', 'ClutchToNest','Contact',
      'ContactPosition', 'Crawl', 'CrawlContact', 'CrawlPredator', 'Hatcheries',
      'Immerging', 'Location', 'Nest', 'Organization', 'Specie', 'TurtleEvent', 'Regions']

for table_name in table_names:
    query ="SELECT * FROM %s" %(table_name)
    df = pd.read_sql(query, engine)
    globals()['df_%s' % table_name] = df

```

clean Contact null names and create a full name column


```python
### orginizing and cleaning Contacts dataframe

### --- Generate FullName column without null values.

df_Contact['FullName']= df_Contact['ContactFname'].fillna('') + ' ' + df_Contact['ContactLname'].fillna('')
df_Contacts_clean = df_Contact[['ContactId', 'ContactIDNum', 'ContactPosition', 'BeachSurveyor',
       'NestRelocator', 'NestExcavator', 'ContactGender', 'ContactE-mail',
       'ContactNotes','FullName']]


### test- check for nan FullNames:
if df_Contacts_clean['FullName'].isnull().values.any():
    print ('there are null values!')


### --- Adding position and organization to contacts

df_ContactPosition_Organization = pd.merge(df_ContactPosition, df_Organization, left_on='Organization', right_on='OrganizationID', how='left')
df_Contacts_Position_Organization = pd.merge(df_Contacts_clean, df_ContactPosition_Organization, left_on='ContactPosition', right_on='PositionID', how='left')


### re assigning to original variable:
df_Contact = df_Contacts_Position_Organization
```


```python

```


```python

```


```python
### location df

df_Regions_clean = df_Regions.drop(['RegionSite', 'RegionNorth', 'RegionSouth', 'RegionEast', 'RegionWest'], axis=1)
df_Location_Regions = pd.merge(df_Location, df_Regions_clean, left_on='Region', right_on='RegionId', how='left')
# print(df_Location.shape[0] == df_Location_Regions.shape[0])

### re assigning to original variable:
df_Location = df_Location_Regions
```


```python
### Crawls df
# 1 --- adding species, location to crawls ---

df_Crawl_clean = df_Crawl.drop(['RegionalCrawlID'], axis=1)
df_Specie_clean = df_Specie.drop(['SpeciePic'], axis=1)
df_Crawl_Specie = pd.merge(df_Crawl_clean, df_Specie_clean, left_on='SpecieID', right_on='SpecieId',how='left')
df_Crawl_Specie_Location = pd.merge(df_Crawl_Specie, df_Location, left_on= 'Location', right_on='LocationID', how='left')





```


```python
# 2 --- adding contacts info ---


def GetContact(crawlcontact_id):
    contact = df_Contact.loc[df_Contact['ContactId'] == crawlcontact_id]
    # contact_name = contact['FullName'].item()
    return contact


df_Crawl_Specie_Location_Contacts = df_Crawl_Specie_Location

crawl_ids_counter = Counter(df_CrawlContact.CrawlID)
for crawl_id in crawl_ids_counter.keys():
    more_contacts_str =''
    contacts_ids=df_CrawlContact.loc[df_CrawlContact.CrawlID == crawl_id]
    contact_list = []

    for crawlcontact_id in contacts_ids['ContactID']:
        contact = df_Contact.loc[df_Contact['ContactId'] == crawlcontact_id]
        contact_name = contact['FullName'].item()
        contact_list.append(contact_name)

    main_contact_name = contact_list[0]
    if len(contact_list)>0:
        more_contacts_str = ', '.join(contact_list[1:])

    updating_index = np.where(df_Crawl_Specie_Location_Contacts['CrawlID']==crawl_id)[0][0]
    df_Crawl_Specie_Location_Contacts.at[updating_index,'other_observers_names'] = more_contacts_str
    df_Crawl_Specie_Location_Contacts.at[updating_index,'main_observer_name'] = main_contact_name
    df_Crawl_Specie_Location_Contacts.at[updating_index, 'CrawlID']

    contacts_ids_list = []
    for i in contacts_ids['ContactID']:
        contacts_ids_list.append(str(i))
    contacts_ids_str = ', '.join(contacts_ids_list)
    df_Crawl_Specie_Location_Contacts.at[updating_index, 'contacts_ids'] = contacts_ids_str

#     print('new CrawlID: ', df_Crawl_Specie_Location_Contacts.at[updating_index, 'CrawlID'])
#     print('new main_observer_name: ',df_Crawl_Specie_Location_Contacts.at[updating_index, 'main_observer_name'])
#     print('new other_observers_names: ',df_Crawl_Specie_Location_Contacts.at[updating_index, 'other_observers_names'])
#     print('new contacts_ids: ',df_Crawl_Specie_Location_Contacts.at[updating_index, 'contacts_ids'])
#     print('-------------------------')
#     print('')



### re assigning to original variable:
# df_Crawl = df_Crawl_Specie_Location
```


```python
# 3 --- adding predators ---


df_Crawl_Specie_Location_Contacts_Predators = df_Crawl_Specie_Location_Contacts
crawl_predators_ids_counter = Counter(df_CrawlPredator.CrawlID)

for crawl_id in crawl_predators_ids_counter.keys():
    if not crawl_id >0:
        print('is a null values', crawl_id)
    else:
        crawl_predators = df_CrawlPredator.loc[df_CrawlPredator.CrawlID == crawl_id]
        crawl_predators_list = []

        for crawl_predator in crawl_predators['PredatorTrack']:
            if crawl_predator == None:
                print('--------------------crawlpredator == None---------------------------')
            else:
                updating_index = np.where(df_Crawl_Specie_Location_Contacts_Predators['CrawlID']==crawl_id)[0][0]
                df_Crawl_Specie_Location_Contacts_Predators.at[updating_index,'predation'] = "yes"
                df_Crawl_Specie_Location_Contacts_Predators.at[updating_index,crawl_predator] = 1


```

    is a null values nan
    is a null values nan
    is a null values nan
    --------------------crawlpredator == None---------------------------
    


```python
# 4 --- reassigning to original df variable ---

df_Crawl = df_Crawl_Specie_Location_Contacts_Predators
# df_Crawl
```


```python

```


```python

```

## Inspecting data issues

#### Checking for duplications:
When a turtle is going on shore to lay eggs - that's a **crawl** .

Somethimes she's going on shore without laying eggs. 

If there are eggs - then that is a **clutch** (or rarely 2 clutches).

Then the bioligists will decide - if to keep the clutch or to take it to a hatchery....

if one crawl is connected to few clutches - we need to inspect it more because it's not normal.


![image](https://user-images.githubusercontent.com/42611120/213127091-879bf85e-b99c-4698-bfcc-f80608dbc7ff.png)



```python
##inspect data
# df_Crawl_Specie_Location_Contacts.columns ## which columns there are
# df_Crawl.Nest.unique() ## what values this col contains


# are there *Clutches* connected to same *Crawls*?:

print(df_Clutches.duplicated(subset='CrawlID').sum()) ###duplications of crawl ids in clutches df

# if not df_Clutches.shape[0]==df_Clutches.CrawlID.nunique():
#     print(df_Clutches.shape[0])
#     print(df_Clutches.CrawlID.nunique())

```

    40
    

It means that few CrawlIDs appear few times -meaning that *few clutches have the same Crawl ID*.

or anther way to put is- few cases where one Crawl is connected to few Clutches. 

#### visually inspecting duplicates in clutches and nests

I'll investigate these clutches - are they similar or identical? or are there any differences?
Is there any important data in these clutches that I should keep?


```python
## all rows with same CrawlID:
df_Clutches.loc[df_Clutches.CrawlID.duplicated(keep=False)].sort_values('CrawlID').drop(['CollectingDate','RelocationDate' ],axis=1)  ##dropping sensitive data


```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ClutchID</th>
      <th>CrawlID</th>
      <th>EggsNo</th>
      <th>DeptTopEgg</th>
      <th>DepthBottomEgg</th>
      <th>NestDiameter</th>
      <th>NestDistance</th>
      <th>BrokenOnRelocation</th>
      <th>Predation</th>
      <th>PreyedEggs</th>
      <th>CollectionHour</th>
      <th>CollectorID</th>
      <th>RelocationHour</th>
      <th>RelocatorID</th>
      <th>WasRelocated</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1312</th>
      <td>1807</td>
      <td>5072</td>
      <td>94.0</td>
      <td>36.0</td>
      <td>60.0</td>
      <td>22.0</td>
      <td>26.0</td>
      <td>0.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>331.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1400</th>
      <td>1975</td>
      <td>5072</td>
      <td>94.0</td>
      <td>36.0</td>
      <td>60.0</td>
      <td>22.0</td>
      <td>26.0</td>
      <td>0.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>1899-12-30 09:00:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1343</th>
      <td>1838</td>
      <td>5103</td>
      <td>116.0</td>
      <td>20.0</td>
      <td>52.0</td>
      <td>25.0</td>
      <td>12.0</td>
      <td>5.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 15:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 17:00:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1401</th>
      <td>2062</td>
      <td>5103</td>
      <td>116.0</td>
      <td>20.0</td>
      <td>52.0</td>
      <td>25.0</td>
      <td>12.0</td>
      <td>5.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 15:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 17:00:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1963</th>
      <td>3231</td>
      <td>161696</td>
      <td>120.0</td>
      <td>31.0</td>
      <td>51.0</td>
      <td>23.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>211.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>211.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1808</th>
      <td>2978</td>
      <td>161720</td>
      <td>84.0</td>
      <td>36.0</td>
      <td>53.0</td>
      <td>22.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:40:00</td>
      <td>487.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1836</th>
      <td>3006</td>
      <td>161750</td>
      <td>78.0</td>
      <td>40.0</td>
      <td>56.0</td>
      <td>24.0</td>
      <td>16.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>250.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>250.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1835</th>
      <td>3005</td>
      <td>161750</td>
      <td>78.0</td>
      <td>40.0</td>
      <td>56.0</td>
      <td>24.0</td>
      <td>16.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>250.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>250.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1962</th>
      <td>3195</td>
      <td>162103</td>
      <td>91.0</td>
      <td>35.0</td>
      <td>50.0</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>237.0</td>
      <td>1899-12-30 09:30:00</td>
      <td>237.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2025</th>
      <td>3293</td>
      <td>162103</td>
      <td>91.0</td>
      <td>35.0</td>
      <td>50.0</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>237.0</td>
      <td>1899-12-30 09:30:00</td>
      <td>237.0</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
<p>62 rows × 15 columns</p>
</div>



 

clutches seems indeed identical (except for less important of data of RelocatorID in clutchid=1807).

I'll check if theses clutches are leading to the same nests - if they do - then I can disgard them.


```python
duplicated_clutches_crawlids=np.unique(df_Clutches.loc[df_Clutches.CrawlID.duplicated()]['CrawlID'].values)

for crawlid in duplicated_clutches_crawlids:
    crawl= df_Crawl.loc[df_Crawl.CrawlID==crawlid]
    clutches = df_Clutches.loc[df_Clutches.CrawlID==crawlid]
    clutchids= clutches['ClutchID'].values
    print('crawlid : ', crawl['CrawlID'].values,)

    for clutchid in clutchids:
        nestids=df_ClutchToNest.loc[df_ClutchToNest.ClutchID==clutchid]['NestID'].values
        print('clutch id: ', clutchid,'\t' 'nestids: ',nestids)
    print('\n')    

```

    crawlid :  [5072]
    clutch id:  1807 	nestids:  [2598]
    clutch id:  1975 	nestids:  []
    
    
    crawlid :  [5103]
    clutch id:  1838 	nestids:  [2628 2630]
    clutch id:  2062 	nestids:  []
    
    
    crawlid :  [161696]
    clutch id:  2961 	nestids:  [3498 3615 3774]
    clutch id:  3051 	nestids:  [3498 3615 3774]
    clutch id:  3231 	nestids:  [3498 3774 3615]
    
    
    crawlid :  [161698]
    clutch id:  2962 	nestids:  [3500 3617 3776]
    clutch id:  3052 	nestids:  [3500 3617 3776]
    clutch id:  3232 	nestids:  [3500 3776 3617]
    
    
    crawlid :  [161700]
    clutch id:  2963 	nestids:  [3518 3673 3832]
    clutch id:  3053 	nestids:  [3518 3673 3832]
    clutch id:  3233 	nestids:  [3518 3673 3832]
    
    
    crawlid :  [161701]
    clutch id:  2964 	nestids:  [3502 3619 3778]
    clutch id:  3054 	nestids:  [3502 3619 3778]
    clutch id:  3234 	nestids:  [3502 3778 3619]
    
    
    crawlid :  [161702]
    clutch id:  2965 	nestids:  [3519 3674 3833]
    clutch id:  3055 	nestids:  [3519 3674 3833]
    clutch id:  3235 	nestids:  [3519 3674 3833]
    
    
    crawlid :  [161703]
    clutch id:  2966 	nestids:  [3503 3620 3779]
    clutch id:  3056 	nestids:  [3503 3620 3779]
    clutch id:  3236 	nestids:  [3503 3779 3620]
    
    
    crawlid :  [161704]
    clutch id:  2967 	nestids:  [3504 3621 3780]
    clutch id:  3057 	nestids:  [3504 3621 3780]
    clutch id:  3237 	nestids:  [3504 3780 3621]
    
    
    crawlid :  [161706]
    clutch id:  2968 	nestids:  [3520 3675 3834]
    clutch id:  3058 	nestids:  [3520 3675 3834]
    clutch id:  3238 	nestids:  [3520 3675 3834]
    
    
    crawlid :  [161707]
    clutch id:  2969 	nestids:  [3521 3676 3835]
    clutch id:  3059 	nestids:  [3521 3676 3835]
    clutch id:  3239 	nestids:  [3521 3676 3835]
    
    
    crawlid :  [161708]
    clutch id:  2970 	nestids:  [3509 3626 3785]
    clutch id:  3060 	nestids:  [3509 3626 3785]
    clutch id:  3240 	nestids:  [3509 3626 3785]
    
    
    crawlid :  [161710]
    clutch id:  2971 	nestids:  [3522 3677 3836]
    clutch id:  3061 	nestids:  [3522 3677 3836]
    clutch id:  3241 	nestids:  [3522 3677 3836]
    
    
    crawlid :  [161714]
    clutch id:  2972 	nestids:  [3514 3631 3790]
    clutch id:  3062 	nestids:  [3514 3631 3790]
    clutch id:  3242 	nestids:  [3514 3631 3790]
    
    
    crawlid :  [161715]
    clutch id:  2973 	nestids:  [3515 3632 3791]
    clutch id:  3063 	nestids:  [3515 3632 3791]
    clutch id:  3243 	nestids:  [3515 3632 3791]
    
    
    crawlid :  [161716]
    clutch id:  2974 	nestids:  [3516 3633 3792]
    clutch id:  3064 	nestids:  [3516 3633 3792]
    clutch id:  3244 	nestids:  [3516 3633 3792]
    
    
    crawlid :  [161717]
    clutch id:  2975 	nestids:  [3517 3634 3793]
    clutch id:  3065 	nestids:  [3517 3634 3793]
    clutch id:  3245 	nestids:  [3517 3634 3793]
    
    
    crawlid :  [161718]
    clutch id:  2976 	nestids:  [3506 3623 3782]
    clutch id:  3066 	nestids:  [3506 3623 3782]
    clutch id:  3246 	nestids:  [3506 3782 3623]
    
    
    crawlid :  [161719]
    clutch id:  2977 	nestids:  [3507 3624 3783]
    clutch id:  3067 	nestids:  [3507 3624 3783]
    clutch id:  3247 	nestids:  [3507 3624 3783]
    
    
    crawlid :  [161720]
    clutch id:  2978 	nestids:  [3508 3625 3784]
    clutch id:  3068 	nestids:  [3508 3625 3784]
    clutch id:  3248 	nestids:  [3508 3625 3784]
    
    
    crawlid :  [161750]
    clutch id:  3005 	nestids:  [3557]
    clutch id:  3006 	nestids:  [3557]
    
    
    crawlid :  [162103]
    clutch id:  3195 	nestids:  [3845]
    clutch id:  3293 	nestids:  [3845]
    
    
    

Each crawl leads to 3 clutches - and each of them is leading to the same 2-3 nests!


Except for crawl_ids: 5103, 5072 - where I should keep clutches 1838 and 1807, and remove clutches 2062 and 1975.

So I can remove duplications in Clutches database, except for clutches 1838 and 1807 - I need to keep them (or add the connection to nest in the second one)





```python
df_Clutches_without_duplicates=df_Clutches.drop_duplicates(subset='CrawlID',keep='first') 

## making sure that clutches 1838 and 1807 are kept
for clutchid in [1838,1807]:
    print(clutchid in df_Clutches_without_duplicates['ClutchID'])

```

    True
    True
    


```python
# --- reassigning to original clutches df ---
print(df_Clutches.shape)

df_Clutches = df_Clutches_without_duplicates
print(df_Clutches.shape)

```

    (2176, 17)
    (2136, 17)
    


```python
print(df_Nest.duplicated().sum())

```

    0
    

In df_Crawls - it's noted if there's a nest or not in column 'Nest'



```python
df_Crawl.loc[df_Crawl.Nest==False].drop(['CrawlDate','North','East','Location'],axis=1)  ##dropping sensitive data

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CrawlID</th>
      <th>SerialNo</th>
      <th>Nest</th>
      <th>ListingDate</th>
      <th>OnlyYear</th>
      <th>DescriptionOfCrawl</th>
      <th>SpecieID</th>
      <th>NumOfDigs</th>
      <th>TrackMin</th>
      <th>TrackMax</th>
      <th>...</th>
      <th>contacts_ids</th>
      <th>predation</th>
      <th>Fox</th>
      <th>Dog</th>
      <th>Man</th>
      <th>unknown</th>
      <th>crab</th>
      <th>Crow</th>
      <th>Cat</th>
      <th>Crab</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>31</th>
      <td>33</td>
      <td>7.0</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>2.0</td>
      <td>4.0</td>
      <td>NaN</td>
      <td>75.0</td>
      <td>...</td>
      <td>268</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>32</th>
      <td>34</td>
      <td>8.0</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>2.0</td>
      <td>5.0</td>
      <td>NaN</td>
      <td>75.0</td>
      <td>...</td>
      <td>268</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>34</th>
      <td>36</td>
      <td>12.0</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>2.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>88.0</td>
      <td>...</td>
      <td>463</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>35</th>
      <td>37</td>
      <td>14.0</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>כפר גלים</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>86.0</td>
      <td>...</td>
      <td>268</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>36</th>
      <td>38</td>
      <td>18.0</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>2.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>62.0</td>
      <td>...</td>
      <td>463</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>4137</th>
      <td>162427</td>
      <td>NaN</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>70.0</td>
      <td>70.0</td>
      <td>...</td>
      <td>494</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4138</th>
      <td>162428</td>
      <td>NaN</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>70.0</td>
      <td>70.0</td>
      <td>...</td>
      <td>494</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4139</th>
      <td>162429</td>
      <td>NaN</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>8.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>836</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4140</th>
      <td>162430</td>
      <td>NaN</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>8.0</td>
      <td>1.0</td>
      <td>80.0</td>
      <td>80.0</td>
      <td>...</td>
      <td>836</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4141</th>
      <td>162431</td>
      <td>NaN</td>
      <td>False</td>
      <td>NaT</td>
      <td>False</td>
      <td>None</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>75.0</td>
      <td>77.0</td>
      <td>...</td>
      <td>494</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>2023 rows × 43 columns</p>
</div>



are all these crawls indeed lead to no clutch?

In df_Clutches:
If there's a nest - it is noted if the nest was relocated or not in column 'WasRelocated'.

Are there crawls without nest that was found in the clutches tabke and also noted as relocated?


```python
crawls_without_nest=df_Crawl.loc[df_Crawl.Nest==False]['CrawlID'].values

crawls_without_nests_and_relocated = df_Clutches[df_Clutches.CrawlID.isin(crawls_without_nest)& df_Clutches.WasRelocated==True]
crawls_without_nests_and_relocated
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ClutchID</th>
      <th>CrawlID</th>
      <th>EggsNo</th>
      <th>DeptTopEgg</th>
      <th>DepthBottomEgg</th>
      <th>NestDiameter</th>
      <th>NestDistance</th>
      <th>BrokenOnRelocation</th>
      <th>Predation</th>
      <th>PreyedEggs</th>
      <th>CollectingDate</th>
      <th>CollectionHour</th>
      <th>CollectorID</th>
      <th>RelocationDate</th>
      <th>RelocationHour</th>
      <th>RelocatorID</th>
      <th>WasRelocated</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>977</th>
      <td>989</td>
      <td>4060</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>0.0</td>
      <td>2007-06-24</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1455</th>
      <td>2143</td>
      <td>5607</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1457</th>
      <td>2145</td>
      <td>5609</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1462</th>
      <td>2150</td>
      <td>5614</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1463</th>
      <td>2151</td>
      <td>5615</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1464</th>
      <td>2152</td>
      <td>5616</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1465</th>
      <td>2153</td>
      <td>5617</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1467</th>
      <td>2155</td>
      <td>5619</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1471</th>
      <td>2159</td>
      <td>5623</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1515</th>
      <td>2203</td>
      <td>5674</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1516</th>
      <td>2204</td>
      <td>5675</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1517</th>
      <td>2205</td>
      <td>5676</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>



It seems that there are clutches that were relocated!
To make sure - I'll check if they are really appearing in the clutchtonest table


```python
clutchids= crawls_without_nests_and_relocated['ClutchID'].values

df_ClutchToNest.loc[df_ClutchToNest.ClutchID.isin(clutchids)].shape
```




    (0, 4)



They don't lead to a nest - meaning that there is no nest even if we fount them in the clutches table. 


```python

```


```python
df_Clutches
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ClutchID</th>
      <th>CrawlID</th>
      <th>EggsNo</th>
      <th>DeptTopEgg</th>
      <th>DepthBottomEgg</th>
      <th>NestDiameter</th>
      <th>NestDistance</th>
      <th>BrokenOnRelocation</th>
      <th>Predation</th>
      <th>PreyedEggs</th>
      <th>CollectingDate</th>
      <th>CollectionHour</th>
      <th>CollectorID</th>
      <th>RelocationDate</th>
      <th>RelocationHour</th>
      <th>RelocatorID</th>
      <th>WasRelocated</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>195</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>196</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>197</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>198</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>False</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>199</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>False</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2171</th>
      <td>3440</td>
      <td>162485</td>
      <td>77.0</td>
      <td>35.0</td>
      <td>48.0</td>
      <td>17.0</td>
      <td>19.0</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>2015-07-04</td>
      <td>1899-12-30 09:00:00</td>
      <td>1024.0</td>
      <td>2015-07-04</td>
      <td>1899-12-30 12:15:00</td>
      <td>1024.0</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2172</th>
      <td>3441</td>
      <td>162486</td>
      <td>72.0</td>
      <td>30.0</td>
      <td>49.0</td>
      <td>14.0</td>
      <td>19.0</td>
      <td>1.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>2015-07-04</td>
      <td>1899-12-30 07:50:00</td>
      <td>1024.0</td>
      <td>2015-07-04</td>
      <td>1899-12-30 13:00:00</td>
      <td>1024.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2173</th>
      <td>3442</td>
      <td>162487</td>
      <td>72.0</td>
      <td>30.0</td>
      <td>49.0</td>
      <td>14.0</td>
      <td>15.0</td>
      <td>1.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>2015-07-04</td>
      <td>1899-12-30 07:50:00</td>
      <td>1024.0</td>
      <td>2015-07-04</td>
      <td>1899-12-30 13:00:00</td>
      <td>1024.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2174</th>
      <td>3443</td>
      <td>162488</td>
      <td>65.0</td>
      <td>35.0</td>
      <td>52.0</td>
      <td>22.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>2015-07-13</td>
      <td>1899-12-30 07:00:00</td>
      <td>397.0</td>
      <td>2015-07-13</td>
      <td>1899-12-30 09:00:00</td>
      <td>397.0</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2175</th>
      <td>3444</td>
      <td>162489</td>
      <td>52.0</td>
      <td>22.0</td>
      <td>45.0</td>
      <td>15.0</td>
      <td>25.0</td>
      <td>0.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>2015-07-13</td>
      <td>1899-12-30 06:00:00</td>
      <td>397.0</td>
      <td>2015-07-13</td>
      <td>1899-12-30 09:00:00</td>
      <td>397.0</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
<p>2136 rows × 17 columns</p>
</div>




```python
### inspect Nests df. are there nests that are not unique?
print(df_Nest.NestID.nunique(), df_Nest.shape)
```

    2266 (2266, 16)
    


```python
### inspect Clutch-to-Nests df. are there clutches that point to the same nest? 
print(df_ClutchToNest.NestID.nunique(), df_ClutchToNest.shape)

```

    2111 (2283, 4)
    


```python
df_ClutchToNest.NestID.duplicated().sum()
```




    172




```python
df_ClutchToNest[df_ClutchToNest.NestID.duplicated(keep=False)].sort_values('NestID')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ClutchToNestID</th>
      <th>ClutchID</th>
      <th>NestID</th>
      <th>EggsRelocated</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1139</th>
      <td>1287</td>
      <td>1526</td>
      <td>2379</td>
      <td>48.0</td>
    </tr>
    <tr>
      <th>1140</th>
      <td>1288</td>
      <td>1529</td>
      <td>2379</td>
      <td>27.0</td>
    </tr>
    <tr>
      <th>1200</th>
      <td>1348</td>
      <td>1534</td>
      <td>2440</td>
      <td>69.0</td>
    </tr>
    <tr>
      <th>1251</th>
      <td>1404</td>
      <td>1535</td>
      <td>2440</td>
      <td>34.0</td>
    </tr>
    <tr>
      <th>1208</th>
      <td>1357</td>
      <td>1565</td>
      <td>2449</td>
      <td>69.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2121</th>
      <td>3515</td>
      <td>2971</td>
      <td>3836</td>
      <td>71.0</td>
    </tr>
    <tr>
      <th>2122</th>
      <td>3516</td>
      <td>3061</td>
      <td>3836</td>
      <td>71.0</td>
    </tr>
    <tr>
      <th>2143</th>
      <td>3559</td>
      <td>3293</td>
      <td>3845</td>
      <td>91.0</td>
    </tr>
    <tr>
      <th>2142</th>
      <td>3558</td>
      <td>3294</td>
      <td>3845</td>
      <td>91.0</td>
    </tr>
    <tr>
      <th>2144</th>
      <td>3560</td>
      <td>3195</td>
      <td>3845</td>
      <td>91.0</td>
    </tr>
  </tbody>
</table>
<p>235 rows × 4 columns</p>
</div>




```python
nestids= set(df_ClutchToNest[df_ClutchToNest.NestID.duplicated(keep=False)]['NestID'])
df_Nest[df_Nest.NestID.isin(nestids)].sort_values('NestID').drop(['OpeningDate','NestAddingDate'], axis=1) ##dropping sensitive data



```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>NestID</th>
      <th>HatcheryNestNo</th>
      <th>HatcheryID</th>
      <th>HatchingDate</th>
      <th>OpeningHour</th>
      <th>OpenerID</th>
      <th>OpeningNotes</th>
      <th>EmptyShells</th>
      <th>LiveInNest</th>
      <th>DeadInNest</th>
      <th>DeadInEggs</th>
      <th>UndevelopedEggs</th>
      <th>NestOwner</th>
      <th>NestOwnerCode</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1105</th>
      <td>2379</td>
      <td>12</td>
      <td>2.0</td>
      <td>NaT</td>
      <td>1899-12-30 18:15:00</td>
      <td>260.0</td>
      <td>None</td>
      <td>51.0</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>0.0</td>
      <td>20.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1165</th>
      <td>2440</td>
      <td>10</td>
      <td>4.0</td>
      <td>NaT</td>
      <td>1899-12-30 19:30:00</td>
      <td>385.0</td>
      <td>None</td>
      <td>57.0</td>
      <td>1.0</td>
      <td>8.0</td>
      <td>13.0</td>
      <td>26.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1173</th>
      <td>2449</td>
      <td>18</td>
      <td>4.0</td>
      <td>NaT</td>
      <td>1899-12-30 21:45:00</td>
      <td>385.0</td>
      <td>None</td>
      <td>33.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>9.0</td>
      <td>35.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1221</th>
      <td>2498</td>
      <td>23</td>
      <td>8.0</td>
      <td>NaT</td>
      <td>1899-12-30 16:30:00</td>
      <td>331.0</td>
      <td>None</td>
      <td>66.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1372</th>
      <td>2659</td>
      <td>18</td>
      <td>4.0</td>
      <td>NaT</td>
      <td>1899-12-30 20:00:00</td>
      <td>395.0</td>
      <td>None</td>
      <td>87.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>5.0</td>
      <td>2626.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2020</th>
      <td>3833</td>
      <td>11</td>
      <td>8.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>4.0</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>2798.0</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>3834</td>
      <td>18</td>
      <td>8.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>None</td>
      <td>72.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>8.0</td>
      <td>7.0</td>
      <td>2805.0</td>
    </tr>
    <tr>
      <th>2022</th>
      <td>3835</td>
      <td>19</td>
      <td>8.0</td>
      <td>NaT</td>
      <td>1899-12-30 18:30:00</td>
      <td>NaN</td>
      <td>None</td>
      <td>40.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>5.0</td>
      <td>18.0</td>
      <td>7.0</td>
      <td>2806.0</td>
    </tr>
    <tr>
      <th>2023</th>
      <td>3836</td>
      <td>22</td>
      <td>8.0</td>
      <td>NaT</td>
      <td>1899-12-30 18:30:00</td>
      <td>NaN</td>
      <td>None</td>
      <td>65.0</td>
      <td>3.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>5.0</td>
      <td>7.0</td>
      <td>2811.0</td>
    </tr>
    <tr>
      <th>2028</th>
      <td>3845</td>
      <td>1</td>
      <td>11.0</td>
      <td>NaT</td>
      <td>1899-12-30 06:10:00</td>
      <td>835.0</td>
      <td>None</td>
      <td>74.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>14.0</td>
      <td>3.0</td>
      <td>2746.0</td>
    </tr>
  </tbody>
</table>
<p>63 rows × 14 columns</p>
</div>




```python
clutchids= set(df_ClutchToNest[df_ClutchToNest.NestID.duplicated(keep=False)]['ClutchID'])
df_Clutches.loc[df_Clutches.ClutchID.isin(clutchids)].sort_values('ClutchID').drop(['CollectingDate','RelocationDate'],axis=1) ##dropping sensitive data

# for clutchid in clutchids:
#     nestids=df_ClutchToNest.loc[df_ClutchToNest.ClutchID==clutchid]['NestID'].values
#     print('clutch id: ', clutchid,'\t' 'nestids: ',nestids)
# print('\n') 
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ClutchID</th>
      <th>CrawlID</th>
      <th>EggsNo</th>
      <th>DeptTopEgg</th>
      <th>DepthBottomEgg</th>
      <th>NestDiameter</th>
      <th>NestDistance</th>
      <th>BrokenOnRelocation</th>
      <th>Predation</th>
      <th>PreyedEggs</th>
      <th>CollectionHour</th>
      <th>CollectorID</th>
      <th>RelocationHour</th>
      <th>RelocatorID</th>
      <th>WasRelocated</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1103</th>
      <td>1526</td>
      <td>4702</td>
      <td>53.0</td>
      <td>35.0</td>
      <td>47.0</td>
      <td>15.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>Partially preyed</td>
      <td>5.0</td>
      <td>1899-12-30 06:00:00</td>
      <td>260.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>260.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1106</th>
      <td>1529</td>
      <td>4705</td>
      <td>27.0</td>
      <td>25.0</td>
      <td>35.0</td>
      <td>15.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 10:30:00</td>
      <td>260.0</td>
      <td>1899-12-30 11:30:00</td>
      <td>260.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1111</th>
      <td>1534</td>
      <td>4710</td>
      <td>69.0</td>
      <td>23.0</td>
      <td>47.0</td>
      <td>18.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>320.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>320.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1112</th>
      <td>1535</td>
      <td>4711</td>
      <td>54.0</td>
      <td>30.0</td>
      <td>44.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Partially preyed</td>
      <td>20.0</td>
      <td>1899-12-30 05:45:00</td>
      <td>320.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>320.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1140</th>
      <td>1563</td>
      <td>4739</td>
      <td>NaN</td>
      <td>40.0</td>
      <td>50.0</td>
      <td>19.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Partially preyed</td>
      <td>NaN</td>
      <td>1899-12-30 09:00:00</td>
      <td>228.0</td>
      <td>1899-12-30 10:30:00</td>
      <td>228.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1142</th>
      <td>1565</td>
      <td>4741</td>
      <td>69.0</td>
      <td>30.0</td>
      <td>50.0</td>
      <td>20.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 08:15:00</td>
      <td>345.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>345.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1194</th>
      <td>1618</td>
      <td>4794</td>
      <td>58.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Partially preyed</td>
      <td>50.0</td>
      <td>NaT</td>
      <td>331.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1195</th>
      <td>1619</td>
      <td>4795</td>
      <td>61.0</td>
      <td>38.0</td>
      <td>58.0</td>
      <td>19.0</td>
      <td>18.0</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1364</th>
      <td>1859</td>
      <td>5261</td>
      <td>93.0</td>
      <td>15.0</td>
      <td>48.0</td>
      <td>20.0</td>
      <td>16.0</td>
      <td>2.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 06:30:00</td>
      <td>385.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>385.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1365</th>
      <td>1860</td>
      <td>5262</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Partially preyed</td>
      <td>NaN</td>
      <td>1899-12-30 03:00:00</td>
      <td>385.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>385.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1490</th>
      <td>2178</td>
      <td>5642</td>
      <td>114.0</td>
      <td>40.0</td>
      <td>60.0</td>
      <td>24.0</td>
      <td>15.0</td>
      <td>NaN</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1491</th>
      <td>2179</td>
      <td>5643</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>20.0</td>
      <td>NaN</td>
      <td>Partially preyed</td>
      <td>35.0</td>
      <td>1899-12-30 06:20:00</td>
      <td>331.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1519</th>
      <td>2207</td>
      <td>5678</td>
      <td>9.0</td>
      <td>38.0</td>
      <td>45.0</td>
      <td>25.0</td>
      <td>25.0</td>
      <td>0.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 06:35:00</td>
      <td>720.0</td>
      <td>1899-12-30 09:30:00</td>
      <td>720.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1541</th>
      <td>2229</td>
      <td>5716</td>
      <td>68.0</td>
      <td>48.0</td>
      <td>56.0</td>
      <td>25.0</td>
      <td>25.0</td>
      <td>0.0</td>
      <td>Not preyed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>395.0</td>
      <td>1899-12-30 12:00:00</td>
      <td>395.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1791</th>
      <td>2961</td>
      <td>161696</td>
      <td>120.0</td>
      <td>31.0</td>
      <td>51.0</td>
      <td>23.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>211.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>211.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1792</th>
      <td>2962</td>
      <td>161698</td>
      <td>77.0</td>
      <td>25.0</td>
      <td>40.0</td>
      <td>25.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>211.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>211.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1793</th>
      <td>2963</td>
      <td>161700</td>
      <td>104.0</td>
      <td>35.0</td>
      <td>50.0</td>
      <td>15.0</td>
      <td>30.0</td>
      <td>1.0</td>
      <td>Partially prayed</td>
      <td>0.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>352.0</td>
      <td>1899-12-30 10:30:00</td>
      <td>352.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1794</th>
      <td>2964</td>
      <td>161701</td>
      <td>85.0</td>
      <td>30.0</td>
      <td>54.0</td>
      <td>20.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 07:15:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1795</th>
      <td>2965</td>
      <td>161702</td>
      <td>64.0</td>
      <td>28.0</td>
      <td>50.0</td>
      <td>18.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:00:00</td>
      <td>1019.0</td>
      <td>1899-12-30 10:15:00</td>
      <td>385.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1796</th>
      <td>2966</td>
      <td>161703</td>
      <td>87.0</td>
      <td>42.0</td>
      <td>51.0</td>
      <td>21.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>1019.0</td>
      <td>1899-12-30 10:30:00</td>
      <td>1019.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1797</th>
      <td>2967</td>
      <td>161704</td>
      <td>81.0</td>
      <td>33.0</td>
      <td>47.0</td>
      <td>20.0</td>
      <td>25.0</td>
      <td>0.0</td>
      <td>Partially prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>1019.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>1019.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1798</th>
      <td>2968</td>
      <td>161706</td>
      <td>81.0</td>
      <td>22.0</td>
      <td>45.0</td>
      <td>17.0</td>
      <td>25.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 12:30:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1799</th>
      <td>2969</td>
      <td>161707</td>
      <td>65.0</td>
      <td>40.0</td>
      <td>50.0</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>NaN</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1800</th>
      <td>2970</td>
      <td>161708</td>
      <td>63.0</td>
      <td>32.0</td>
      <td>53.0</td>
      <td>21.0</td>
      <td>26.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 08:00:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1801</th>
      <td>2971</td>
      <td>161710</td>
      <td>71.0</td>
      <td>44.0</td>
      <td>58.0</td>
      <td>23.0</td>
      <td>42.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>1899-12-30 09:10:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1802</th>
      <td>2972</td>
      <td>161714</td>
      <td>58.0</td>
      <td>10.0</td>
      <td>40.0</td>
      <td>180.0</td>
      <td>15.0</td>
      <td>3.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 12:15:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1803</th>
      <td>2973</td>
      <td>161715</td>
      <td>43.0</td>
      <td>42.0</td>
      <td>52.0</td>
      <td>74.0</td>
      <td>23.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 06:20:00</td>
      <td>NaN</td>
      <td>1899-12-30 07:30:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1804</th>
      <td>2974</td>
      <td>161716</td>
      <td>105.0</td>
      <td>58.0</td>
      <td>71.0</td>
      <td>27.0</td>
      <td>13.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 18:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 19:00:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1805</th>
      <td>2975</td>
      <td>161717</td>
      <td>82.0</td>
      <td>32.0</td>
      <td>58.0</td>
      <td>20.0</td>
      <td>20.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1806</th>
      <td>2976</td>
      <td>161718</td>
      <td>86.0</td>
      <td>28.0</td>
      <td>56.0</td>
      <td>21.0</td>
      <td>21.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>1019.0</td>
      <td>NaT</td>
      <td>1019.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1807</th>
      <td>2977</td>
      <td>161719</td>
      <td>65.0</td>
      <td>33.0</td>
      <td>45.0</td>
      <td>22.0</td>
      <td>31.0</td>
      <td>1.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1808</th>
      <td>2978</td>
      <td>161720</td>
      <td>84.0</td>
      <td>36.0</td>
      <td>53.0</td>
      <td>22.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:40:00</td>
      <td>487.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1835</th>
      <td>3005</td>
      <td>161750</td>
      <td>78.0</td>
      <td>40.0</td>
      <td>56.0</td>
      <td>24.0</td>
      <td>16.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>250.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>250.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1962</th>
      <td>3195</td>
      <td>162103</td>
      <td>91.0</td>
      <td>35.0</td>
      <td>50.0</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>237.0</td>
      <td>1899-12-30 09:30:00</td>
      <td>237.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1982</th>
      <td>3250</td>
      <td>162168</td>
      <td>120.0</td>
      <td>31.0</td>
      <td>51.0</td>
      <td>23.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>211.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>211.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1983</th>
      <td>3251</td>
      <td>162170</td>
      <td>77.0</td>
      <td>25.0</td>
      <td>40.0</td>
      <td>25.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>211.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>211.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1984</th>
      <td>3252</td>
      <td>162172</td>
      <td>104.0</td>
      <td>35.0</td>
      <td>50.0</td>
      <td>15.0</td>
      <td>30.0</td>
      <td>1.0</td>
      <td>Partially prayed</td>
      <td>0.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>352.0</td>
      <td>1899-12-30 10:30:00</td>
      <td>352.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1985</th>
      <td>3253</td>
      <td>162173</td>
      <td>85.0</td>
      <td>30.0</td>
      <td>54.0</td>
      <td>20.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 07:15:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1986</th>
      <td>3254</td>
      <td>162174</td>
      <td>64.0</td>
      <td>28.0</td>
      <td>50.0</td>
      <td>18.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:00:00</td>
      <td>1019.0</td>
      <td>1899-12-30 10:15:00</td>
      <td>385.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1987</th>
      <td>3255</td>
      <td>162175</td>
      <td>87.0</td>
      <td>42.0</td>
      <td>51.0</td>
      <td>21.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>1019.0</td>
      <td>1899-12-30 10:30:00</td>
      <td>1019.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1988</th>
      <td>3256</td>
      <td>162176</td>
      <td>81.0</td>
      <td>33.0</td>
      <td>47.0</td>
      <td>20.0</td>
      <td>25.0</td>
      <td>0.0</td>
      <td>Partially prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>1019.0</td>
      <td>1899-12-30 10:00:00</td>
      <td>1019.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1989</th>
      <td>3257</td>
      <td>162178</td>
      <td>81.0</td>
      <td>22.0</td>
      <td>45.0</td>
      <td>17.0</td>
      <td>25.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 12:30:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1990</th>
      <td>3258</td>
      <td>162179</td>
      <td>65.0</td>
      <td>40.0</td>
      <td>50.0</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>NaN</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1991</th>
      <td>3259</td>
      <td>162180</td>
      <td>63.0</td>
      <td>32.0</td>
      <td>53.0</td>
      <td>21.0</td>
      <td>26.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 08:00:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1992</th>
      <td>3260</td>
      <td>162182</td>
      <td>71.0</td>
      <td>44.0</td>
      <td>58.0</td>
      <td>23.0</td>
      <td>42.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>1899-12-30 09:10:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1993</th>
      <td>3261</td>
      <td>162186</td>
      <td>58.0</td>
      <td>10.0</td>
      <td>40.0</td>
      <td>180.0</td>
      <td>15.0</td>
      <td>3.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 09:00:00</td>
      <td>NaN</td>
      <td>1899-12-30 12:15:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1994</th>
      <td>3262</td>
      <td>162187</td>
      <td>43.0</td>
      <td>42.0</td>
      <td>52.0</td>
      <td>74.0</td>
      <td>23.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 06:20:00</td>
      <td>NaN</td>
      <td>1899-12-30 07:30:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1995</th>
      <td>3263</td>
      <td>162188</td>
      <td>105.0</td>
      <td>58.0</td>
      <td>71.0</td>
      <td>27.0</td>
      <td>13.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 18:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 19:00:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1996</th>
      <td>3264</td>
      <td>162189</td>
      <td>82.0</td>
      <td>32.0</td>
      <td>58.0</td>
      <td>20.0</td>
      <td>20.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:00:00</td>
      <td>331.0</td>
      <td>1899-12-30 07:30:00</td>
      <td>331.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2022</th>
      <td>3290</td>
      <td>162232</td>
      <td>86.0</td>
      <td>28.0</td>
      <td>56.0</td>
      <td>21.0</td>
      <td>21.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>1019.0</td>
      <td>NaT</td>
      <td>1019.0</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2023</th>
      <td>3291</td>
      <td>162233</td>
      <td>65.0</td>
      <td>33.0</td>
      <td>45.0</td>
      <td>22.0</td>
      <td>31.0</td>
      <td>1.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2024</th>
      <td>3292</td>
      <td>162234</td>
      <td>84.0</td>
      <td>36.0</td>
      <td>53.0</td>
      <td>22.0</td>
      <td>33.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 07:40:00</td>
      <td>487.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>NaN</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2026</th>
      <td>3294</td>
      <td>162235</td>
      <td>91.0</td>
      <td>35.0</td>
      <td>50.0</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>0.0</td>
      <td>Not prayed</td>
      <td>0.0</td>
      <td>1899-12-30 08:30:00</td>
      <td>237.0</td>
      <td>1899-12-30 09:30:00</td>
      <td>237.0</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>



it seems that clutches that was removed from Clutches table - still apears in the Nests table, even if not leading to any data because not asked..
it should be removed/noted as irelevant data.



```python
# clutchids=df_Clutches.loc[df_Clutches.CrawlID.isin(duplicated_clutches_crawlids)]['ClutchID'].values
# print(clutchids)
deleted_crawlids = duplicated_clutches_crawlids

unique_clutchids = df_Clutches['ClutchID'].values

for clutchid in deleted_crawlids:
    if clutchid in unique_clutchids:
        np.delete(deleted_crawlids, np.argwhere(deleted_crawlids == clutchid))
        
print(clutchids)
print(deleted_crawlids)        
print(duplicated_clutches_crawlids)



```

    {1529, 1563, 1565, 3064, 1618, 1619, 3195, 2178, 2179, 2207, 3231, 3232, 3234, 3233, 3236, 3237, 3235, 3238, 3240, 3239, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3241, 3250, 3251, 3252, 2229, 3253, 3255, 3256, 3254, 3257, 3259, 3258, 3261, 3262, 3263, 3264, 3260, 3290, 3291, 3292, 3293, 3294, 1859, 1860, 2961, 2962, 2963, 2964, 2965, 2966, 2967, 2968, 2969, 2970, 2971, 2972, 2973, 2974, 2975, 2976, 2977, 2978, 3005, 3006, 3051, 3052, 3053, 3054, 3055, 3056, 3057, 3058, 3059, 3060, 3061, 1526, 3063, 3062, 3065, 3066, 3067, 3068, 1534, 1535}
    [  5072   5103 161696 161698 161700 161701 161702 161703 161704 161706
     161707 161708 161710 161714 161715 161716 161717 161718 161719 161720
     161750 162103]
    [  5072   5103 161696 161698 161700 161701 161702 161703 161704 161706
     161707 161708 161710 161714 161715 161716 161717 161718 161719 161720
     161750 162103]
    


```python

```


```python

```


```python

```


```python

```

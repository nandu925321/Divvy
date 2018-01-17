import pandas as pd
import numpy as np
import geopy.distance
from datetime import datetime
from geopy.geocoders import Nominatim
from collections import defaultdict
import multiprocessing
import random
import string
#% matplotlib inline

weather = {}
data1 = pd.read_csv("Divvy_Trips_2017_Q1.csv")
data2 = pd.read_csv("Divvy_Trips_2017_Q2.csv")
data = pd.concat([data1,data2])
#data = data1
station_data = pd.read_csv("Divvy_Stations_2017_Q1Q2.csv")

print("Processing Q1", data.shape)

data = data.dropna()

data.drop(['from_station_name','to_station_name','trip_id','bikeid'], axis = 1,inplace=True)
station_data.set_index('id', inplace=True)
sd=station_data.to_dict('id')

data['distance'] = 0
data['datetime'] = 0
data['month_rel'] = 0
data['day_rel'] = 0
data['hr_rel'] = 0
data['max_temp'] = 0
data['min_temp'] = 0
data['avg_temp'] = 0

print(data.head(5))

def process(df):
	da = df
	index = 0
	for (idx,row) in da.iterrows():
		index+=1
		#print(index)
		c1 = (sd[row.loc['from_station_id']])
		c2 = (sd[row.loc['to_station_id']])
		#geolocator = Nominatim()
		#coord = str(c1['latitude'])+","+" "+str(c1['longitude'])
		#location = geolocator.reverse(coord)
		#zone = list(location.address.split(","))
		#zone = [z.strip() for z in zone]
		#if "Chicago" not in zone:
			#print("here")
		#	da.drop(da.index[[0]],inplace=True)
		#	continue
		# add weather



		coords_1 = (c1['latitude'], c1['longitude'])
		coords_2 = (c2['latitude'], c2['longitude'])
		dis = geopy.distance.vincenty(coords_1, coords_2).km
		if dis<=2:
			da.at[idx,'distance']=1

		if row.loc['usertype'].strip() == "Subscriber":
			da.at[idx,'usertype']=1
		else:
			da.at[idx,'usertype']=0
		if row.loc['gender'].strip() == 'Male':
			da.at[idx,'gender']=1
		else:
			da.at[idx,'gender']=0

		date_string = row.loc['start_time']
		datetime_object = datetime.strptime(date_string,"%m/%d/%Y %H:%M:%S")
		da.at[idx,'datetime'] = datetime_object
		day = datetime_object.weekday()
		hour = datetime_object.hour
		month = datetime_object.month
		all_weather = weather.get(month,[0,0,0]).get(day, [0,0,0])
		#if all_weather is None:
		#	all_weather = [0,0,0]

		da.at[idx,'max_temp'] = all_weather[0]
		da.at[idx,'min_temp'] = all_weather[1]
		da.at[idx,'avg_temp'] = all_weather[2]


		if month == 12:
			da.at[idx,'month_rel'] = 0
		else:
			da.at[idx,'month_rel'] = month

		da.at[idx,'day_rel'] = day

		da.at[idx,'hr_rel'] = hour
		da.at[idx,'tripduration'] = float(row.loc['tripduration'])/60

		da.at[idx,'birthyear'] = float(2017 - row.loc['birthyear'])
	file_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))+".csv"
	da.to_csv(file_name, sep=',')
	print(str(multiprocessing.current_process())+"completed")

def process_weather():
	wea = pd.read_excel("chicagow.xlsx")

	for (idx,row) in wea.iterrows():
		date_string = row.loc['Date']
		#datetime_object = datetime.strptime(date_string,"%Y-%m-%d")
		month = date_string.month
		if month not in weather:
			weather[month] = {}
		weather[month][date_string.day] = [row.loc['Maximum'],row.loc['Minimum'],row.loc['Average']]

process_weather()
#print(weather)

if __name__ == '__main__':
	#configurable options.  different values may work better.
	numthreads = 8
	numlines = 100000
	ser = []
	i=0
	for g, df in data.groupby(np.arange(len(data)) // 100000):
		i +=1
		print(df.shape)
		#print(df.head(5))
		ser.append(df)
		#if i>2:
		#	break
	pool = multiprocessing.Pool(processes=numthreads)
	pool.map(process, (a for a in ser))
    #lines[line:line+numlines] for line in range(0,len(lines),numlines) )


    


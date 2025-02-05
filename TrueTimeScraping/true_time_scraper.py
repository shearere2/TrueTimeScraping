import requests
from datetime import datetime
import pytz
import pandas as pd
import time

class VehicleScraper:

    def __init__(self):
        self.link = 'https://truetime.portauthority.org/gtfsrt-bus/vehicles?debug'

    def get_current_vehicle_locations(self) -> pd.DataFrame:
        response = requests.get(self.link)
        vehicles = response.content

        collection_stamp = int(vehicles.split()[7].decode('utf-8'))
        collection_stamps = [datetime.fromtimestamp(collection_stamp, tz=pytz.timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")]

        vehicles = vehicles.split(bytes('entity {'.encode('utf-8')))
        vehicles = [x.decode('utf-8') for x in vehicles][1:]

        dates, timestamps, vehicle_ids, lats, lons, bearings, speeds, trip_ids, sched_relats, route_ids = [],[],[],[],[],[],[],[],[],[]

        for info in vehicles[1:]:
            temp = info

            info = info[info.find('timestamp: ')+11:]
            timestamp = int(info[:info.find('\n')])
            vehicle_id = info[info.find('id: ')+5:info.find('"\n')]

            timestamps.append(timestamp)

            dates.append(datetime.fromtimestamp(timestamp, tz=pytz.timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"))
            vehicle_ids.append(vehicle_id)

            info = temp
            info = info[info.find('position {\n')+11:]
            info = info[:info.find('}')]

            lat = float(info[info.find('latitude: ')+10:info.find('longitude: ')])
            lon = float(info[info.find('longitude: ')+11:info.find('bearing: ')])
            bearing = float(info[info.find('bearing: ')+8:info.find('speed: ')])
            speed = float(info[info.find('speed: ')+7:])

            lats.append(lat)
            lons.append(lon)
            bearings.append(bearing)
            speeds.append(speed)

            info = temp

            info = info[info.find('trip {')+7:]
            info = info[:info.find('}')]

            trip_id = info[info.find('trip_id: ')+10:info.find('"\n')]
            schedule_relationship = info[info.find('schedule_relationship: ')+23:info.find('route_id: ')-8]
            route_id = info[info.find('route_id: ')+11:-6]
            if len(route_id)>6:
                route_id = 'off_route'

            trip_ids.append(trip_id)
            sched_relats.append(schedule_relationship)
            route_ids.append(route_id)

        return pd.DataFrame({'date_time':dates, 'vehicle_id':vehicle_ids, 'latitude':lats,
                            'longitude':lons, 'bearing':bearings, 'speed':speeds,
                            'route_id':route_ids, 'timestamp':timestamps,
                            'collection_timestamp':collection_stamps*len(vehicle_ids)})# , 'trip_id':trip_ids, 'schedule_relationship':sched_relats, 'route_id':route_ids})

    def get_curr_vehicle_counts(self):
        locs = self.get_current_vehicle_locations()
        timestamps = locs.groupby('route_id').apply(lambda x: x['timestamp'].mean(), include_groups=False)
        groups = locs.groupby('route_id').apply(lambda x: len(x), include_groups=False)
        sum = 0
        routes = []
        bus_count = []
        for group in groups.index:
            sum+=groups[group]
            routes.append(group)
            bus_count.append(groups[group])

        return pd.DataFrame({'bus_count':bus_count, 'timestamp':timestamps, 'collection_stamp':[locs['collection_timestamp'][0]]*len(timestamps)}).reset_index().rename(columns={'route_id':'Route'})

    def collect_data_each_minute(self): # Time limit of 5 minutes, so 5 collections
        count = 0
        # Keeps only route name from file, not interested in hand collected data anymore
        vehicle_counts_main = pd.read_csv(r'C:\Users\Ethan Shearer\VSCodeProjects\TrueTimeScraping\data\vehicles_per_route_estimates.csv')[['Route']]
        
        while count < 10:
            df = self.get_curr_vehicle_counts()
            df[df['collection_stamp'][0]] = df['bus_count']
            df = df.drop(columns=['timestamp', 'bus_count', 'collection_stamp'])
            vehicle_counts_main = pd.merge(vehicle_counts_main, df, on='Route')
            count += 1
            time.sleep(30)

        return vehicle_counts_main
    
class TripScraper:

    def __init__(self):
        self.link = 'https://truetime.portauthority.org/gtfsrt-bus/trips?debug'

    def get_current_trips_info(self) -> pd.DataFrame:
        response = requests.get(self.link)
        trips = response.content
        trips = trips.split(bytes('entity {'.encode('utf-8')))
        trips = [x.decode('utf-8') for x in trips][1:]

        trip_ids, sched_relats, route_ids, vehicle_ids, stop_ids, times, stop_sequences = [],[],[],[],[],[],[]

        for trip in trips:
            curr = 0

            for stop in trip[trip.find('stop_time_update {'):trip.find('vehicle {')].split('stop_time_update {')[1:]:
                curr += 1
                s = stop.find('stop_sequence: ')+14
                stop_sequence = int(stop[s:stop.find('\n', s)])
                stop_sequences.append(stop_sequence)

                s = stop.find('arrival {')+10
                try: 
                    timestamp = int(stop[stop.find('time: ', s)+6:stop.find('\n', s)])
                    time = datetime.fromtimestamp(timestamp, tz=pytz.timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
                except:
                    time = 0
                times.append(time)

                s = stop.find('stop_id:')+10
                stop_id = stop[s:stop.find('"', s)]
                stop_ids.append(stop_id)

                s = stop.find('schedule_relationship: ')+22
                sched_relat = stop[s:stop.find('\n', s)].strip()
                sched_relats.append(sched_relat)

            temp = trip
            trip = trip[trip.find('trip {')+7:trip.find('}')]

            trip_id = trip[trip.find('trip_id: ')+10:trip.find('\n')-1]
            #   Unused                        sched_relat = trip[trip.find('schedule_relationship: ')+23:trip.find('route_id: ')-8]
            route_id = trip[trip.find('route_id: ')+11:-6]

            trip_ids.extend([trip_id]*curr)
            #   Unused                        sched_relats.extend([sched_relat]*len(stop_ids))
            route_ids.extend([route_id]*curr)

            trip = temp

            trip[trip.find('vehicle {')+10:]

            start = trip.find('vehicle {')+10
            vehicle_ids.extend([trip[trip.find('id: "', start)+5:trip.find('}', start)-6]]*curr)
            

        return pd.DataFrame({'vehicle_id':vehicle_ids, 'trip_id':trip_ids,
                'schedule_relationship':sched_relat, 'route_id':route_ids,
                'stop_id':stop_ids, 'time':times, 'stop_sequence':stop_sequences})

if __name__ == "__main__":
    try:
        full_data = pd.read_csv(r'C:\Users\Ethan Shearer\VSCodeProjects\TrueTimeScraping\data\true_time_data.csv')
        scraper = VehicleScraper()
        df = scraper.collect_data_each_minute()
        df = pd.merge(full_data, df, on='Route').set_index('Route')
    except:
        scraper = VehicleScraper()
        df = scraper.collect_data_each_minute().set_index('Route')
    # Add data to already existing file (needs fixed)
    df.to_csv(r'C:\Users\Ethan Shearer\VSCodeProjects\TrueTimeScraping\data\true_time_data.csv', mode='w')
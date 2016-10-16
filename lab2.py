import logging
import requests
import time
import json
import re
logging.basicConfig(level=logging.DEBUG)
from spyne import Application, rpc, ServiceBase, \
    String, Unicode, Decimal
from spyne import Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication
from operator import itemgetter

class HelloWorldService(ServiceBase):
    @rpc(Decimal, Decimal, Decimal,_returns=String)
    def checkcrime(ctx, lat, lon, radius):
        
        payload = {'lat': lat, 'lon': lon,'radius': radius, 'key':'.'}
        crimeDetails = requests.get('https://api.spotcrime.com/crimes.json', params=payload)
        arr_crime = []
        arr_crime = crimeDetails.json()

        other_Type = []
        robbery_Type = []
        burglary_Type = []
        theft_Type = []
        assault_Type = []
        arrest_Type = []

        streets = {}

        for crimeType in arr_crime["crimes"]:
            if crimeType['type'] == "Other":
                other_Type.append(crimeType)
            elif crimeType['type'] == "Robbery":
                robbery_Type.append(crimeType)
            elif crimeType['type'] == "Burglary":
                burglary_Type.append(crimeType)
            elif crimeType['type'] == "Theft":
                theft_Type.append(crimeType)
            elif crimeType['type'] == "Assault":
                assault_Type.append(crimeType)
            elif crimeType['type'] == "Arrest":
                arrest_Type.append(crimeType)

        slot1_s = time.strptime("12:01 AM", "%I:%M %p")
        slot1_e = time.strptime("03:00 AM", "%I:%M %p")

        slot2_s = time.strptime("03:01 AM", "%I:%M %p")
        slot2_e = time.strptime("06:00 AM", "%I:%M %p")

        slot3_s = time.strptime("06:01 AM", "%I:%M %p")
        slot3_e = time.strptime("09:00 AM", "%I:%M %p")

        slot4_s = time.strptime("09:01 AM", "%I:%M %p")
        slot4_e = time.strptime("11:59 AM", "%I:%M %p")
        slot4_sp = time.strptime("12:00 PM", "%I:%M %p")

        slot5_s = time.strptime("12:01 PM", "%I:%M %p")
        slot5_e = time.strptime("03:00 PM", "%I:%M %p")

        slot6_s = time.strptime("03:01 PM", "%I:%M %p")
        slot6_e = time.strptime("06:00 PM", "%I:%M %p")

        slot7_s = time.strptime("06:01 PM", "%I:%M %p")
        slot7_e = time.strptime("09:00 PM", "%I:%M %p")

        slot8_s = time.strptime("09:01 PM", "%I:%M %p")
        slot8_e = time.strptime("11:59 PM", "%I:%M %p")
        slot8_sp = time.strptime("12:00 AM", "%I:%M %p")

        slot1 = []
        slot2 = []
        slot3 = []
        slot4 = []
        slot5 = []
        slot6 = []
        slot7 = []
        slot8 = []
        
        for crimeTime in arr_crime["crimes"]:
            if time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot1_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot1_e:
                slot1.append(crimeTime)
            elif time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot2_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot2_e:
                slot2.append(crimeTime)
            elif time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot3_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot3_e:
                slot3.append(crimeTime)
            elif (time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot4_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot4_e) or (time.strptime(crimeTime['date'][9:], "%I:%M %p") == slot4_sp):
                slot4.append(crimeTime)
            elif time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot5_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot5_e:
                slot5.append(crimeTime)
            elif time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot6_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot6_e:
                slot6.append(crimeTime)
            elif time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot7_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot7_e:
                slot7.append(crimeTime)
            elif (time.strptime(crimeTime['date'][9:], "%I:%M %p") >= slot8_s and time.strptime(crimeTime['date'][9:], "%I:%M %p") <= slot8_e) or (time.strptime(crimeTime['date'][9:], "%I:%M %p") == slot8_sp):
                slot8.append(crimeTime)    
        
        result_String = {
            'total_crime': len(arr_crime["crimes"]),
            'the_most_dangerous_streets' : [],
            'crime_type_count' : {
                'Other': len(other_Type),
                'Robbery' : len(robbery_Type),
                'Theft': len(theft_Type),
                'Burglary': len(burglary_Type),
                'Assault' : len(assault_Type),
                'Arrest' : len(arrest_Type)
            },
            'event_time_count' : {
            "12:01am-3am" : len(slot1),
            "3:01am-6am" : len(slot2),
            "6:01am-9am" : len(slot3),
            "9:01am-12noon" : len(slot4),
            "12:01pm-3pm" : len(slot5),
            "3:01pm-6pm" : len(slot6),
            "6:01pm-9pm" : len(slot7),
            "9:01pm-12midnight" : len(slot8)
            }
            }
        
        temp_streets = []
        for crimeStreet in arr_crime["crimes"]:
            if crimeStreet["address"].find(' & ')!=-1:
                street = crimeStreet["address"].split(' & ')                
                for st in street:
                    temp_streets.append(st)
            else:
                temp_streets.append(crimeStreet["address"])
            for st in temp_streets:
                if st.find('BLOCK OF')!=-1:
                    st = st.split('BLOCK OF ')[1]
                if st.rfind('BLOCK ')!=-1:
                    st =st[st.rfind('BLOCK ') + 6:]
                streets[st] = streets.get(st, 0) + 1
        street1 = ""
        street2 = ""
        street3 = ""
        i = 0
        arr = streets
        for st_elem in streets:
            if i==0:
                street1 = st_elem
                i += 1
                continue
            if i==1:
                street2 = st_elem
                if arr[street2]>arr[street1]:
                    temp = street2
                    street2 = street1
                    street1 = temp
                i += 1
                continue
            if i==2:
                street3 = st_elem
                if arr[street3] > arr[street2]:
                    temp = street3
                    street3 = street2
                    street2 = temp
                if arr[street2] > arr[street1]:
                    temp = street2
                    street2 = street1
                    street1 = temp
                i += 1
                continue
            if arr[st_elem] > arr[street3]:
                street3 = st_elem
                if arr[street3] > arr[street2]:
                    temp = street3
                    street3 = street2
                    street2 = temp
                if arr[street2] > arr[street1]:
                    temp = street2
                    street2 = street1
                    street1 = temp
                i += 1
                continue
            i += 1
        result_String["the_most_dangerous_streets"] = [street1, street2, street3]
                    
        yield result_String
        
application = Application([HelloWorldService],
    tns='spyne.examples.hello',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()

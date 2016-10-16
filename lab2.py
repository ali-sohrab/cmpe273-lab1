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
        
        

        pStreets = []
        for crimeStreet in arr_crime["crimes"]:
            if crimeStreet["address"].find(' & ')!=-1:
                street = crimeStreet["address"].split(' & ')                
                for st in street:
                    pStreets.append(st)
            else:
                pStreets.append(crimeStreet["address"])
            for st in pStreets:
                if st.find('BLOCK OF')!=-1:
                    st = st.split('BLOCK OF ')[1]
                if st.rfind('BLOCK ')!=-1:
                    st =st[st.rfind('BLOCK ') + 6:]
                streets[st] = streets.get(st, 0) + 1
        st1 = ""
        st2 = ""
        st3 = ""
        i = 0
        arr = streets
        for elem in streets:
            if i==0:
                st1 = elem
                i += 1
                continue
            if i==1:
                st2 = elem
                if arr[st2]>arr[st1]:
                    temp = st2
                    st2 = st1
                    st1 = temp
                i += 1
                continue
            if i==2:
                st3 = elem
                if arr[st3] > arr[st2]:
                    temp = st3
                    st3 = st2
                    st2 = temp
                if arr[st2] > arr[st1]:
                    temp = st2
                    st2 = st1
                    st1 = temp
                i += 1
                continue
            if arr[elem] > arr[st3]:
                st3 = elem
                if arr[st3] > arr[st2]:
                    temp = st3
                    st3 = st2
                    st2 = temp
                if arr[st2] > arr[st1]:
                    temp = st2
                    st2 = st1
                    st1 = temp
                i += 1
                continue
            i += 1
        result_String["the_most_dangerous_streets"] = [st1, st2, st3]
        #return result_String
            
        yield result_String
        
        '''
            for key in arr_crime['crimes']:

                address = re.split(r'\bBLOCK', key["address"])
                if re.search(r'\bST', address[0]):
                    temp1 = address[0].strip().replace("OF ", "")
                    tmp1 = re.split("&", temp1)   
                    street_list.append(tmp1[0].strip())
                if (len(tmp1) > 1):
                    street_list.append(tmp1[1].strip())
                if len(address) > 1:
                    temp1 = address[1].strip().replace("OF ", "")
                    tmp2 = temp1.split("&")
                    street_list.append(tmp2[0].strip())
                if (len(tmp2) > 1):
                    street_list.append(tmp2[1].strip())
                if len(address) > 2:
                    temp1 = address[2].strip().replace("OF ", "")
                    tmp3 = temp1.split("&")
                    street_list.append(tmp3[0].strip())
                if (len(tmp3) > 1):
                    street_list.append(tmp3[1].strip())
                    street_counter = {}
            
            for street in street_list:
                if street in street_counter:
                    street_counter[street] += 1
                else:
                    street_counter[street] = 1

            popular_street = sorted(street_counter, key=street_counter.get, reverse=True)

            top_3 = popular_street[:3]
        '''    




        '''
        streets = []

        strt = [res_crimeType for res_crimeType in res_crime["crimes"] if len(res_crimeType['address']) > 0 ]
            #for res in res_crimeType:
        #yield strt
        for res in strt:
            if res['address'].find(' BLOCK ',0,len(res['address'])):
                res1 = res['address'].split(' BLOCK ')
                if res1['address'].find(' BLOCK ',0,len(res['address'])):
                    res1n = res1['address'].split(' BLOCK ')
                #streets.append(res2[0])
                print 'res1 %s ' % res1n
            if res['address'].find(' OF ',0,len(res['address'])):
                res2 = res['address'].split(' OF ')
                #streets.append(res1[0])
                print 'res2 %s ' % res2
            #streets.append(res1[1])
            if res['address'].find(' & ',0,len(res['address'])):#streets.append(res1[1])
                res3 =  res['address'].split(' & ')
                print 'res3 %s ' % res3
                #streets.append(res3[0])
                #streets.append(res3[1])

        yield streets
        yield 'total streets = %s' % len(streets)
        '''
        '''
        pStreets = []
        streets = []
        for crimeStreet in arr_crime["crimes"]:
            if crimeStreet["address"].find(' & ') != -1:
                street = crimeStreet["address"].split(' & ')                
                for st in street:
                    pStreets.append(st)
            else:
                pStreets.append(crimeStreet["address"])
            for st in pStreets:
                if st.find('BLOCK OF') != -1:
                    st = st.split('BLOCK OF ')[1]
                if st.rfind('BLOCK ') != -1:
                    st = st[st.rfind('BLOCK ') + 6:]
                #etc.streets[st] = etc.streets.get(st, 0) + 1
                #streets = streets.append(st)
                i = 0
                if len(streets) == 0:
                    streets.append(st)
                else:
                    for elem in streets:
                        i += 1
                        if elem == st:
                            break
                        elif i<len(streets):
                            continue
                        elif len(streets)<3:
                            streets.append(st)
            pStreets = []
        '''
        


        '''
        streets_final = {}
        streets = [res_crimeType for res_crimeType in arr_crime["crimes"] if len(res_crimeType['address']) > 0]
        
        for street in streets:
            address = street['address']
            if address.find('OF'):
                str1 = address.split('OF')
                d = [s.encode('ascii') for s in str1]    
                for x in d:
                    if len(d)>1:
                        if x not in streets_final:
                            streets_final[d[1]]= 1
                        elif x in streets_final:           
                            streets_final[d[1]] += 1
                    elif len(d)==1:
                        if x not in streets_final:
                            streets_final[x]= 1
                        elif x in streets_final:
                            streets_final[x] += 1       
            elif address.find("&"):
                str2 = address.split("&")
                d = [s.encode('ascii') for s in str2]
                for y in d:
                    if y not in streets_final:
                        streets_final[y]= 1
                    elif y in streets_final:
                        streets_final[y] += 1
        
      
        
        streets_final = sorted( streets_final.items(), key=itemgetter(1), reverse=True)

        dangerous_streets = []
  
        dangerous_streets.append(streets_final[0])
        dangerous_streets.append(streets_final[1])
        dangerous_streets.append(streets_final[2])
        '''
        
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

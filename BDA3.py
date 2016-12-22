from __future__ import division
from pyspark import SparkContext, SparkConf
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
from datetime import datetime

conf = SparkConf().setAppName("lab_kernel")
sc = SparkContext(conf = conf)

#sc = SparkContext(appName="lab_kernel")
def haversine(lon1, lat1, lon2, lat2):
#"""
#Calculate the great circle distance between two points
#on the earth (specified in decimal degrees)
#"""
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

h_distance = 100 # Up to you
h_date = 7# Up to you
h_time = 2 # Up to you
#a = float(58.4274) # Up to you
#b = float(14.826) # Up to you
#date = "2013-07-04" # Up to you
#hour = "01:00:00"

a = 58.409158
b = 15.60452
date = "2016-03-04"
hour = "04:00:00"
my_hours = ("00:00:00", "02:00:00", "04:00:00", "06:00:00", "08:00:00", "10:00:00",\
 "12:00:00", "14:00:00", "16:00:00", "18:00:00", "20:00:00", "22:00:00")

def gauss(x,h):
    return exp( -(abs(x)**2 / 2*h**2 ))

def gauss_time(x,h):
    if x > 12:
        x = x - 12

    return gauss(x,h)



def kernsum(k1,k2,k3,norm,y):
    return sum(k1,k2,k3)*y / norm


stations = sc.textFile("../stations.csv")
stations = stations.map(lambda a: a.split(";"))
temps = sc.textFile("../temps50k.csv")
temps = temps.filter(lambda x: len(x)>0).map(lambda a: a.split(","))
# Your code here

stationsmap = stations.map(lambda a: (a[0],(float(a[3]),float(a[4]) )) )
colstation = stationsmap.collectAsMap()
statval= sc.broadcast(colstation).value



tempsmap = temps.map(lambda a: (a[0],(datetime.strptime(a[1],"%Y-%m-%d") #dagar\
,datetime.strptime(a[2],"%H:%M:%S") #timmar\
,float(a[3]) ) ) ) #temp

tempsmap = tempsmap.filter(lambda a: a[1][0] < datetime.strptime(date+" "+my_hours[0],"%Y-%m-%d %H:%M:%S"))
stm1 = tempsmap.map(lambda x: (x[0],(statval[x[0]][0],statval[x[0]][1],x[1][0],x[1][1],x[1][2])))
#print stm.take(5)a[1][0[0],
#stm1 = stm.map(lambda a: (a[0],(a[1][0][0],a[1][0][1]\
#,a[1][1][0],a[1][1][1],a[1][1][2])  ) )
kernel = []
i = 1
for hour in my_hours:
    stm1 = stm1.filter(lambda x: datetime.combine(datetime.date(x[1][2]),datetime.time(x[1][3])) < datetime.strptime(date+" "+hour,"%Y-%m-%d %H:%M:%S"))

    k1 = stm1.map(lambda x: (x[0], (gauss(haversine(x[1][1],x[1][0],b,a ), h_distance),\
    gauss((x[1][2] - datetime.strptime(date, "%Y-%m-%d")).days,h_date),\
    gauss_time((x[1][3] - datetime.strptime(hour,"%H:%M:%S")).seconds/(60*60) ,h_time),\
    x[1][4] )))


    ksum = k1.map(lambda a: (a[1][0] + a[1][1] + a[1][2] ) ).sum()
    #print ksum

    #kernel = k1.map(lambda x: (kernsum(k1 = a[1][0], k2 = a[1][1], k3 = a[1][2], y = a[1][4], norm = ksum)) )
    kernela = k1.map(lambda x: ( ((x[1][0] + x[1][1] + x[1][2])*x[1][3]) / ksum ) ).sum()

    kernel.append(kernela)

print kernel
#k2 = stm1.map(lambda a: (a[0], gauss((a[3] - datetime.strptime(date, "%Y-%m-%d")).days,h_date) ))
#k3 = stm1.map(lambda a: (a[0], gauss((a[4]-datetime.time(datetime.strptime(hour,"%H:%M:%S"))).seconds/60/60,h_time) ))

#k23 = stm1.map(lambda a: (a[0],\
#gauss((a[3] - datetime.strptime(date, "%Y-%m-%d")).days,h_date),\
 #gauss((a[4]-datetime.strptime(hour,"%H:%M:%S")).seconds/60/60,h_time) ))


#("12",(1,2))
#1+2

#[0],a[1][1],a[2],a[3] ))
#stm = stm.map(lambda a:(a[0],(haversine(a[1][0][0],a[1][0][1],a,b) \
# ,a[2] - datetime.strptime(date,"%Y-%m-%d") \
# ,a[3] - datetime.strptime(hour,"%H:%M:%S") ) ))

#gaussmap = stm.map(lambda a: (a[0],\
#(gauss(a[1],h_distance), gauss(a[2,h_date]), gauss(a[3],h_time) )) )

#st = sc.textFile("../st1.csv")
#stm = st.map(lambda a: a.split(","))

#Det har kommer behovas andras iom att den filen jag anvander
#har radnummer i forsta kolumnen. c(1,4,5,9,10,11) date.hour() str(a[9]) a[10]
#stationsnumer, long,lat,datum,tid,temperatur
#DEN HAR FUNKAR!!!!
#stmap = stm.map(lambda a: (a[1],(a[4],a[5],datetime.strptime(a[9], "%Y-%m-%d"),datetime.strptime(a[10],"%H:%M:%S"),a[11] )) )

#stmap = stm.map(lambda a: (a[1],/
#haversine(float(a[4]),float(a[5]),a,b) ,/
#datetime.strptime("2013-07-04", "%Y-%m-%d"),/
#datetime.strptime("12:00:00","%H:%M:%S"),/
#a[11] ))

#(a[4]-datetime.strptime(h_time,"%H:%M:%S")).seconds/60/60

# funkar i pyhthon
# gauss((datetime.strptime("2013-07-04", "%Y-%m-%d") - datetime.strptime("2013-07-10", "%Y-%m-%d")).days,1)
# gauss((datetime.strptime("2013-07-04", "%Y-%m-%d") - datetime.strptime("date, "%Y-%m-%d")).days,h_date)

#borde ocksa funka, dock ger differensen mellan 12 och 23 en tidsskillnad pa 13 h.
# (datetime.strptime("12:00:00","%H:%M:%S")-datetime.strptime("23:00:00","%H:%M:%S")).seconds/60/60
# gauss(datetime.strptime("12:00:00","%H:%M:%S")-datetime.strptime(time,"%H:%M:%S")).seconds/60/60

#print stationsmap.take(5)
#print tempsmap.take(5)

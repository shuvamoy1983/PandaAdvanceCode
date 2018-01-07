
import re
from datetime import datetime

s = "I am 29 years Old,My name is Pyhon and I want to rule the world for the next 10 Years," \
    "My Date of birth is 10-20-1990 and I will die after 09-19-2010"
d = dict()
v = re.split(',', s)
print(dict(enumerate(tuple(v))))

p = re.findall('[A-Z][a-z]*',s)
print(p)

digit =re.findall('\d{1,3}',s)
print(digit)
date_val =re.findall('[0-9][0-9]-[0-9][0-9]-[[0-9][0-9][0-9][0-9]',s)
for i in date_val:
    print(datetime.date(datetime.strptime(i,'%m-%d-%Y')))

h = 10
k='1990-10-20'
def test(k):
    if k == 10:
        print(k)
    elif k == '1990-10-20':
        print("I am date")
    else:
         pass

print(test(k))
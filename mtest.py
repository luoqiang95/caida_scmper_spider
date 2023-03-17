from datetime import datetime

a = "Wed, 02 Mar 2022 17:14:26 GMT"
b = a.replace(',', '').replace(' GMT', '')
c = datetime.strptime(b, "%a %d %b %Y %H:%M:%S")
print(c)
#

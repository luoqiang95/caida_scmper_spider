from datetime import datetime

a = "Wed 02 Mar 2022 17:14:26"
b = datetime.strptime(a, "%a %d %d %Y %H:%M:%S")
print(b)

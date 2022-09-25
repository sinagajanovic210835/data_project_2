import random
from faker import Faker

countriesWithCode = {}
userIds = set()
products = set()
invoiceNoWithCountryCodes = {}

with open("Csv/country.csv") as file:
    for line in file:
        if line.strip():
            (val, key) = line.strip().split(";")
            countriesWithCode[key] = val

# Change field Country from country name to country code and add random region value from 1 - 6,
# extract user ids and products ids with prices and dates and write to a new file

newFile = open("./Csv/data1.csv", "w")
newFile.write("InvoiceNo;StockCode;Description;Quantity;InvoiceDate;UnitPrice;CustomerID;Country\n")
header = True

with open("./Csv/data.csv", 'rb') as file:
    for line1 in file:
        line = line1.decode(errors='ignore')
        if line.strip() and not header:
            arr = line.strip().split(",")
            length = len(arr)
            country = arr[length - 1].strip()
            userId = arr[length - 2].strip()
            if userId:
                userIds.add(userId)
            invoiceNo = arr[0].strip()
            stockCode = arr[1].strip()
            countryCode = countriesWithCode[country] + "-" + str(random.randint(1, 6))
            if invoiceNo in invoiceNoWithCountryCodes.keys():
                countryCode = invoiceNoWithCountryCodes[invoiceNo]
            else:
                invoiceNoWithCountryCodes[invoiceNo] = countryCode
            description = arr[2].strip()
            if length == 9:
                description = arr[2] + "," + arr[3]
            elif length == 10:
                description = arr[2] + "," + arr[3] + "," + arr[4]
            newLine = invoiceNo + ";" + stockCode + ";" + description.strip().replace('"', "") + ";" + arr[length - 5].strip() + ";" + arr[length - 4].strip() + ";" + arr[length - 3].strip() + ";" + userId + ";" + countryCode + "\n"
            newFile.write(newLine)
            products.add((stockCode, description, arr[length - 3], arr[length - 4].split(" ")[0].strip()))
            # print(arr)
        header = False


file.close()
newFile.close()
fake = Faker()

# Write extracted user ids with generated random names into file

users = open("./Csv/users.csv", "w")
users.write("id;name\n")
for user in userIds:
    userName = fake.name()
    users.write(user + ";" + userName + "\n")
users.close()

# write products into file

prodfile = open("./Csv/products.csv", "w")
prodfile.write("StockCode;Description;UnitPrice;Date\n")
for prod in products:
    line = ""
    for field in prod:
        line = line + field + ";"
    line = line[:-1] + "\n"
    prodfile.write(line)
prodfile.close()

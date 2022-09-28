from time import time, ctime, struct_time
import calendar

cnt = 0
header = True
start = 0
first = True

newFile = open("../Csv/country/country.csv", "r")
with open("../Csv/invoices/invoices.csv") as invoices:
    for line in invoices:
        if line.strip() and not header:
            arr = line.strip().split(";")            
            dt = arr[len(arr) - 3].split(" ")[0]
            tm = arr[len(arr) - 3].split(" ")[1]
            day = int(dt.split("/")[1])
            mnth = int(dt.split("/")[0])
            yr = int(dt.split("/")[2])
            hr = int(tm.split(":")[0])
            minut = int(tm.split(":")[1])
            if first:
                cnt += 1
                newFile.close()
                start = hr
                name = "../Csv/invoicesByHour/" + str(cnt) + "--" + str(start) + ":00-" + str(start + 1) + ":00--" + str(day)  + "-" + str(mnth) + "-" + str(yr) + ".csv"
                newFile = open(name, "w")
                newFile.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country")
                first = False
            if hr == start:
                newFile.write(line.strip() + "\n")
            else:
                first = True
                newFile.close()
        header = False
    invoices.close()
# print("**************************************")
# t = time()
# print(t)
# t1 = struct_time((2010, 1, 12, 8,26,0,0,0,0))

# print(t1)
# t11 = calendar.timegm(t1)
# print(t11)
# t3 = ctime(t11)
# print(t3)

#  6,12/1/2010 8:26      
# with open("Csv/country/country.csv") as countries:
#     for line in file:        

# with open("Csv/products/products.csv") as products:
#     for line in file:
      


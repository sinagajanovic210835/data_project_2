cntrFile = open("../Csv/country/country.csv", "r")
cntrFile.close()
prodFile = open("../Csv/country/country.csv", "r")
prodFile.close()

countriesWithCode = {}
with open("../Csv/country/country.csv") as file:
    for line in file:
        if line.strip():
            (key, val) = line.strip().split(";")
            countriesWithCode[key] = val
    file.close()



cnt = 0
header = True
start = 0
first = True
countries = set()
products = set()
newFile = open("../Csv/country/country.csv", "r")
newFile.close()

# Write invoices in smaller files, hourly separated, etc. all invoices on same date between 8:00 - 9:00 hours

with open("../Csv/invoices/invoices.csv") as invoices:
    for line in invoices:
        if line.strip() and not header:
            arr = line.strip().split(";")            
            dt = arr[-3].split(" ")[0]
            tm = arr[-3].split(" ")[1]
            day = int(dt.split("/")[1])
            mnth = int(dt.split("/")[0])
            yr = int(dt.split("/")[2])
            hr = int(tm.split(":")[0])
            minut = int(tm.split(":")[1])
            countr = arr[-1].split("-")[0]
            countries.add((arr[-1], countriesWithCode[countr]))
            if first:
                cnt += 1                
                newFile.close()
                start = hr
                name = "../Csv/invoicesByHour/" + str(cnt) + "--" + str(start) + ":00-" + str(start + 1) + ":00--" + str(day)  + "-" + str(mnth) + "-" + str(yr) + ".csv"
                newFile = open(name, "w")
                newFile.write("InvoiceNo;StockCode;Quantity;InvoiceDate;CustomerID;Country\n")
                first = False
            if hr == start:
                newFile.write(line.strip() + "\n")
            else:
                first = True
                newFile.close()
        header = False
    invoices.close()


      


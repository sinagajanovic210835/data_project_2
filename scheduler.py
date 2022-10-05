cntrFile = open("./Csv/country/country.csv", "r")
cntrFile.close()
prodFile = open("./Csv/country/country.csv", "r")
prodFile.close()

cnt = 0
header = True
start = 0
first = True
newFile = open("./Csv/country/country.csv", "r")
newFile.close()

# Write invoices in smaller files, hourly separated, etc. all invoices on same date between 8:00 - 9:00 hours

with open("./Csv/invoices/invoices.csv") as invoices:
    for line in invoices:
        if line.strip() and not header:
            arr = line.strip().split("|")            
            dt = arr[-3].split(" ")[0]
            tm = arr[-3].split(" ")[1]
            day = int(dt.split("/")[1])
            mnth = int(dt.split("/")[0])
            yr = int(dt.split("/")[2])
            hr = int(tm.split(":")[0])
            minut = int(tm.split(":")[1])
            
            if first:
                cnt += 1                
                newFile.close()
                start = hr
                name = "./Csv/invoicesByHour/" + str(cnt) + "__" + str(start) + "__" + str(start + 1) + "_00__" + str(day)  + "_" + str(mnth) + "_" + str(yr) + ".csv"
                newFile = open(name, "w")                
                first = False
            if hr == start:
                newFile.write(line.strip() + "\n")
            else:
                first = True
                newFile.close()
        header = False
    invoices.close()

    
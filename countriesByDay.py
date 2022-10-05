countriesWithCode = {}

with open("./Csv/country/country.csv", "r") as file:
    for line in file:
        if line.strip():
            (key, val) = line.strip().split(";")
            countriesWithCode[key] = val
    file.close()

with open("./Csv/invoices/invoices.csv") as file:
    newFile = open("./Csv/country/country.csv", "r")
    newFile.close()
    codesToWrite = set()
    recordededCodes = set()
    header = True
    tmpDate = ""
    newDate = True
    cnt = 0

    for line in file:        
        if line.strip() and not header:
            arr = line.strip().split("|")[3].split(" ")[0].split("/")
            date = arr[1] + "_" +arr[0] + "_" + arr[2]
            code = line.strip().split("|")[-1].split("-")[0]
            
            if newDate:
                tmpDate = date
                newDate = False
            if date == tmpDate:
                if not code in recordededCodes:
                    codesToWrite.add(code)
                    recordededCodes.add(code)
            else:
                if len(codesToWrite) > 0:
                    name = "./Csv/countriesByDay/" + tmpDate + ".csv"
                    newFile = open(name, "w")
                    newFile.write("country_code|country_name\n")
                    for ccd in codesToWrite:
                        newFile.write(ccd + "|" + countriesWithCode[ccd] + "\n")
                    newFile.close
                    codesToWrite = set()
                tmpDate = date         

        header = False   
    file.close()

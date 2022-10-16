with open("./Csv/products/products.csv") as file:
    header = True
    newDate = True
    tmpDate = ""
    cnt = 0
    newFile = open("./Csv/country/country.csv", "r")
    newFile.close()
    line1 = ""

    for line in file:
        if line.strip() and not header:
            arr = line.strip().split("|")[3].split(" ")[0].split("/")
            date = arr[1] + "_" +arr[0] + "_" + arr[2]
            
            if newDate:
                cnt += 1   
                tmpDate = date             
                newDate = False                
                name = "./Csv/productsByDay/" + tmpDate + ".csv"
                newFile = open(name, "w")
                newFile.write(line1)
                newFile.write(line.strip() + "\n")
            elif date == tmpDate:
                newFile.write(line.strip() + "\n")
            else:                
                newDate = True
                line1 = line.strip() + "\n"
                newFile.close()

        header = False
    file.close()
    
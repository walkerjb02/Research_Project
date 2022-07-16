import json
import os
import pandas as pd
import time
cimport cython


cpdef get_date(char* filepath):
    cdef object file
    with open(filepath,"r+") as jsonfile:
        file = json.loads(jsonfile.read())
        return file[file.index("<published>") + 11:file.index("</published>"):]


cpdef get_title(char* filepath):
    cdef object file
    cdef object uncuttitle
    cdef list title
    cdef list ondeck
    cdef int i

    with open(filepath,"r+") as jsonfile:
        file = json.loads(jsonfile.read())
        uncuttitle = file[file.index("<title>") + 7:file.index("</title>"):]
        uncuttitle = uncuttitle.lower()
        title = []
        ondeck = []
        i = 0
        while i <= len(uncuttitle):
            if i == len(uncuttitle):
                title.append("".join(ondeck))
                ondeck = []
            elif " " in uncuttitle[i] or ";" in uncuttitle[i] or ":" in uncuttitle[i]:
                title.append("".join(ondeck))
                ondeck = []
            elif not " " in uncuttitle[i] or ";" in uncuttitle[i] or ":" in uncuttitle[i]:
                ondeck.append(uncuttitle[i])
            i += 1
        return (title)


cpdef get_gvkeywsid(company):
    try:
        df = pd.read_csv("companies_for_nexis.csv")
        ticker = df.iat[list(df["company"]).index(company), 1]

    except ValueError:
        df = pd.read_csv("ws_id.csv")
        ticker = df.iat[list(df["company"]).index(company), 1]
    try:
        gvkeydatabase = pd.read_csv("companies_for_nexis.csv")
        gvkey = str(gvkeydatabase.iat[list(gvkeydatabase["ticker"]).index(ticker), 0])
        check = str(gvkeydatabase.iat[list(gvkeydatabase["ticker"]).index(ticker), 2])
        if company in check:
            pass
        else:
            gvkey = str(gvkeydatabase.iat[list(gvkeydatabase["ticker"]).index(ticker) + 1, 0])
            check1 = str(gvkeydatabase.iat[list(gvkeydatabase["ticker"]).index(ticker) + 1, 2])
            if company in check1:
                pass
            else:
                gvkey = str(gvkeydatabase.iat[list(gvkeydatabase["ticker"]).index(ticker) + 2, 0])
    except ValueError:
        gvkey = False

    try:
        wsid = pd.read_csv("ws_id.csv")
        ws_id = wsid.iat[list(wsid["ticker"]).index(ticker), 0]
        check = wsid.iat[list(wsid["ticker"]).index(ticker), 2]
        if company in check:
            pass
        else:
            check1 = wsid.iat[list(wsid["ticker"]).index(ticker) + 1, 2]
            ws_id = wsid.iat[list(wsid["ticker"]).index(ticker) + 1, 0]
            if company in check1:
                pass
            else:
                ws_id = wsid.iat[list(wsid["ticker"]).index(ticker) + 2, 0]

    except ValueError:
        ws_id = False

    return [gvkey,ws_id]


cpdef get_ticker(char* filepath, company):
    try:
        df = pd.read_csv("companies_for_nexis.csv")
        ticker = df.iat[list(df["company"]).index(company), 1]
    except ValueError:
        df = pd.read_csv("ws_id.csv")
        ticker = df.iat[list(df["company"]).index(company), 1]

    with open(filepath, "r+") as jsonfile:
        file = json.loads(jsonfile.read())
        try:
            if ticker in file[file.index("ticker") + 6::]:
                return "1"
        except ValueError:
            return "0"


cpdef writer(char* filename,char* filepath,company,bint condition):
    cdef object deliverable,filemain
    deliverable = {}
    deliverable["filename"] = filename.decode()
    deliverable["date"] = get_date(filepath=filepath)
    deliverable["title"] = get_title(filepath=filepath)
    deliverable["gvkey"] = get_gvkeywsid(company=company)[0]
    deliverable["ws_id"] = get_gvkeywsid(company=company)[1]
    deliverable["contains_ticker"] = get_ticker(filepath=filepath,company=company)
    deliverable["onewordparse"] = condition
    with open(f"Articles{filename[:4:].decode()}.json","r+") as main:
        filemain = json.loads(main.read())
        filemain["articles"].append(deliverable)
        filemain = json.dumps(filemain)
    with open(f"Articles{filename[:4:].decode()}.json","w") as final:
        final.write(filemain)


cpdef modcompany(char* company):
    cdef int i,lencom
    cdef list ondeck
    cdef bint condition
    cdef list companyname = []
    companyz = company
    uncuttitle = (companyz).decode()
    ondeck = []
    i = 0
    if " " not in uncuttitle:
        companyname.append(companyz.decode())
    else:
        while i <= len(uncuttitle):
            if i == len(uncuttitle):
                companyname.append("".join(ondeck))
                ondeck = []
            elif " " in uncuttitle[i] or ";" in uncuttitle[i] or ":" in uncuttitle[i]:
                companyname.append("".join(ondeck))
                ondeck = []
            elif not " " in uncuttitle[i] or ";" in uncuttitle[i] or ":" in uncuttitle[i]:
                ondeck.append(uncuttitle[i])
            i += 1
        condition = True
        while condition == True and len(companyname) > 1:
            if companyname[-1] == "GROUP" or companyname[-1] == "CO" or companyname[-1] == "COMPANY" or "&,." in companyname[-1] or companyname[-1] in "CORPORATION" or companyname[-1] == "PLC" or companyname[-1] == "SA" or companyname[-1] == "SAS" or companyname[-1] in "INCORPORATED" or companyname[-1] == "LTD" or companyname[-1] == "LIMITED" or companyname[-1] == "S.A." or companyname[-1] == "AB" or "BERHAD" == companyname[-1]:
                del companyname[-1]
            else:
                condition = False

    lencom = len(companyname)
    return ([companyname, lencom])


cpdef main():
    print("Initialized")
    cdef int filedaysindexer,fileindexer,companyindexer,lencompany,indexer
    cdef char* fpbase
    cdef char* fpyear
    cdef char* fpdays
    cdef object americandirectory,directory,filedays,filenames,fp,filename,title
    from Parsestorage import year,filedaysindexer,fileindexer

    fpbase = b"C:\Users\gsbaw\Documents\\targetdir"
    year = year
    americandirectory = list(pd.read_csv("companies_for_nexis.csv")["company"])
    directory = list(pd.read_csv("ws_id.csv")["company"])
    directory.extend(americandirectory)
    bigdirectory = directory

    while year < 2021:
        fpyearz1 = fpbase + b"\\"
        yearz = r"{}".format(year).encode()
        fpyearz2 = fpyearz1 + yearz
        fpyear = fpyearz2
        filedaysindexer = filedaysindexer
        filedays = os.listdir(fpyear.decode())

        while filedaysindexer < len(filedays):
            fpdaysz = fpyear + b"\\" + filedays[filedaysindexer].encode()
            fpdays = fpdaysz
            filenames = os.listdir(fpdays.decode())
            fileindexer = fileindexer

            while fileindexer < len(filenames):
                filenamez = filenames[fileindexer].encode()
                filename = filenamez
                fpz = fpdays + b"\\" + filename
                fp = fpz
                title = get_title(filepath=fp)
                companyindexer = 0

                while companyindexer < len(bigdirectory):
                    companyoutput = modcompany(bigdirectory[companyindexer].encode())
                    company = (" ".join(companyoutput[0]).lower())
                    lencompany = companyoutput[1]
                    indexer = 0

                    try:
                        if lencompany == 1 and company == " ".join(title[indexer:indexer + lencompany:]) and len(
                                company) > 2:

                            writer(filename=filename,filepath=fp,company=(bigdirectory[companyindexer]), condition=True)
                        elif 2 <= lencompany < 4 and companyoutput[0] == title[indexer] and companyoutput[1] == title[
                            indexer + 1] or companyoutput[1] == title[indexer - 1]:

                            writer(filename=filename,filepath=fp,company=(bigdirectory[companyindexer]), condition=False)
                        elif 4 <= lencompany and companyoutput[0] == title[indexer] and companyoutput[1] == title[
                            indexer + 1] or companyoutput[1] == title[indexer - 1] and companyoutput[2] == title[
                            indexer + 2] or companyoutput[2] == title[indexer - 2]:

                            writer(filename=filename,filepath=fp,company=(bigdirectory[companyindexer]), condition=False)
                        else:
                            pass


                    except IndexError:
                        pass

                    indexer += 1

                    companyindexer += 1

                fileindexer += 1
                with open("Parsestorage.py","w") as file:
                    file.write(f"year={year}\nfiledaysindexer={filedaysindexer}\nfileindexer={fileindexer}")

            filedaysindexer += 1
            with open("Parsestorage.py", "w") as file:
                file.write(f"year={year}\nfiledaysindexer={filedaysindexer}\nfileindexer={fileindexer}")
            fileindexer = 0
        print(f'Through {year}')
        year += 1
        with open("Parsestorage.py", "w") as file:
            file.write(f"year={year}\nfiledaysindexer={filedaysindexer}\nfileindexer={fileindexer}")
        filedaysindexer = 0


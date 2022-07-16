cimport cython
import time
import json
import pandas as pd

cpdef get_false():
   cdef char* path
   cdef object file,filen,nfile,toc,tic
   cdef int indexer

   path = b"C:\Users\gsbaw\PycharmProjects\Research\Lib\Articles2020.json"
   with open(path, "r+") as fle:
       file = json.loads(fle.read())
       indexer = 0
       toc = time.perf_counter()
       while indexer < len(file["articles"]):
           try:
               if file["articles"][indexer]["onewordparse"] == True:
                   del file["articles"][indexer]["onewordparse"]
           except KeyError:
               pass
           indexer += 1
       filen = file.copy()
       nfile = json.dumps(filen)
       with open(path,"w") as nfle:
           nfle.write(nfile)

       tic = time.perf_counter()
       print(f"Done in {tic-toc} Seconds")

cpdef get_key():
   cdef char* path
   cdef object file,filen,nfile,toc,tic
   cdef int indexer,fileindexer

   path = b"C:\Users\gsbaw\PycharmProjects\Research\Lib\Articles2020.json"
   with open(path, "r+") as fle:
       file = json.loads(fle.read())
       indexer = 0
       while indexer < len(file["articles"]):
           toc = time.perf_counter()

           if file["articles"][indexer]["ws_id"] == "C156NP700":
                print(file["articles"][fileindexer])
                del file["articles"][indexer]
           if file["articles"][indexer]["ws_id"] == "C826A942A":
                print(file["articles"][fileindexer])
                del file["articles"][indexer]

           if file["articles"][indexer]["ws_id"] == "C356KB600":
                print(file["articles"][fileindexer])
                del file["articles"][indexer]
           indexer += 1

           tic = time.perf_counter()
           print(f"Done in {tic-toc} Seconds")
       filen = file.copy()
       nfile = json.dumps(filen)
       with open(path,"w") as nfle:
           nfle.write(nfile)

cpdef rank_keys():
    cdef int indexer,yearindexer
    cdef object file,wsid
    cdef dict lst

    path = b"C:\Users\gsbaw\PycharmProjects\Research\Lib\Articles"
    yearindexer = 2014
    yearz = "{}.json".format(yearindexer).encode()
    path = path + yearz
    wsid = pd.read_csv("ws_id.csv")
    biglst = {"years":[]}
    while yearindexer < 2021:
        with open(path, "r+") as fle:
           file = json.loads(fle.read())
           indexer = 0
           lst = {}
           while indexer < len(file["articles"]):
                if file["articles"][indexer]["ws_id"] in lst:
                    lst[file["articles"][indexer]["ws_id"]] += 1
                elif file["articles"][indexer]["ws_id"] not in lst:
                    lst[file["articles"][indexer]["ws_id"]] = 1
                elif file["articles"][indexer]["ws_id"] == False:
                    pass
                indexer += 1
           biglst["years"].append(lst)
        print(f"Through {yearindexer}")
        yearindexer += 1
    biglstz = json.dumps(biglst)
    with open("file.json","w") as fal:
        fal.write(biglstz)

cpdef change():
    cdef int year, indexer
    cdef char* needschanged, changeto
    cdef object loaded
    cdef object file
    
    year = 2014
    needschanged = '032520'
    changeto = '0'
    while year < 2021:
        with open(f'Articles{year}.json', 'r+') as file:
            loaded = json.loads(file.read())
            indexer = 0
            while indexer < len(loaded):
                if type(loaded[indexer]["gvkey"]) != bool:
                    if needschanged in loaded[indexer]["gvkey"]:
                        loaded[indexer]["gvkey"] = changeto

                indexer += 1
            final = json.dumps(loaded)
            with open(f'Articles{year}.json', 'w') as nfile:
                nfile.write(final)
        print(year)

        year += 1

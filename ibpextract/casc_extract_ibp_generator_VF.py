import pandas
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

#
#   Extracts the 5 period levels from IBP
#
#   The returned DataFrame contains the following columns
#
#       PERIODID5_TSTAMP : will be used to join with the data extraction
#       WEEK_TECHNICAL
#       WEEK
#       MONTH
#       QUARTER
#       YEAR
#       DATE
#
def _ExtractIbpPeriod(entitySet, timePeriodLevel):
    dataFrameYear = next(ExtractIbpDataGen(entitySet=entitySet, columnList=['PERIODID1','PERIODID1_TSTAMP']))
    startDates = []
    endDates = []
    for index,row in dataFrameYear.iterrows():
        startDate = datetime(row['PERIODID1_TSTAMP'].year, row['PERIODID1_TSTAMP'].month, row['PERIODID1_TSTAMP'].day)
        startDates.append(startDate)
        endDates.append(datetime(row['PERIODID1_TSTAMP'].year, 12, 31))
    dataFrameYear['START_DATE'] = startDates
    dataFrameYear['END_DATE'] = endDates

    dataFrameQuarter = next(ExtractIbpDataGen(entitySet=entitySet, columnList=['PERIODID2','PERIODID2_TSTAMP']))
    startDates = []
    endDates = []
    for index,row in dataFrameQuarter.iterrows():
        startDate = datetime(row['PERIODID2_TSTAMP'].year, row['PERIODID2_TSTAMP'].month, row['PERIODID2_TSTAMP'].day)
        startDates.append(startDate)
        endDates.append(startDate + relativedelta(months=3) - relativedelta(days=1))
    dataFrameQuarter['START_DATE'] = startDates
    dataFrameQuarter['END_DATE'] = endDates
    
    dataFrameMonth = next(ExtractIbpDataGen(entitySet=entitySet, columnList=['PERIODID3','PERIODID3_TSTAMP']))
    startDates = []
    endDates = []
    for index,row in dataFrameMonth.iterrows():
        startDate = datetime(row['PERIODID3_TSTAMP'].year, row['PERIODID3_TSTAMP'].month, row['PERIODID3_TSTAMP'].day)
        startDates.append(startDate)
        endDates.append(startDate + relativedelta(months=1) - relativedelta(days=1))
    dataFrameMonth['START_DATE'] = startDates
    dataFrameMonth['END_DATE'] = endDates
    
    dataFrameWeek = next(ExtractIbpDataGen(entitySet=entitySet, columnList=['PERIODID4','PERIODID4_TSTAMP']))
    startDates = []
    endDates = []
    for index,row in dataFrameWeek.iterrows():
        startDate = datetime(row['PERIODID4_TSTAMP'].year, row['PERIODID4_TSTAMP'].month, row['PERIODID4_TSTAMP'].day)
        startDates.append(startDate)
        endDates.append(startDate + relativedelta(weeks=1) - relativedelta(days=1))
    dataFrameWeek['START_DATE'] = startDates
    dataFrameWeek['END_DATE'] = endDates

    dataFrameTechWeek = next(ExtractIbpDataGen(entitySet=entitySet, columnList=['PERIODID5','PERIODID5_TSTAMP']))

    dataList = []
    #
    #   Loop through Technical Weeks
    #
    addedWeeks = set()
    for index,row in dataFrameTechWeek.iterrows():
        searchDate = datetime(row['PERIODID5_TSTAMP'].year, row['PERIODID5_TSTAMP'].month, row['PERIODID5_TSTAMP'].day)
        #
        #   Add Week
        #
        dfWeekFilter = dataFrameWeek[(dataFrameWeek['START_DATE'] <= searchDate) & (dataFrameWeek['END_DATE'] >= searchDate)]
        periodid4 = dfWeekFilter.iloc[0]['PERIODID4'] if len(dfWeekFilter.index) > 0 else None
        periodid4_tstamp = dfWeekFilter.iloc[0]['PERIODID4_TSTAMP'] if len(dfWeekFilter.index) > 0 else None
        #
        #   Add Month
        #
        dfMonthFilter = dataFrameMonth[(dataFrameMonth['START_DATE'] <= searchDate) & (dataFrameMonth['END_DATE'] >= searchDate)]
        periodid3 = dfMonthFilter.iloc[0]['PERIODID3'] if len(dfMonthFilter.index) > 0 else None
        periodid3_tstamp = dfMonthFilter.iloc[0]['PERIODID3_TSTAMP'] if len(dfMonthFilter.index) > 0 else None
        #
        #   Add Quarter
        #
        dfQuarterFilter = dataFrameQuarter[(dataFrameQuarter['START_DATE'] <= searchDate) & (dataFrameQuarter['END_DATE'] >= searchDate)]
        periodid2 = dfQuarterFilter.iloc[0]['PERIODID2'] if len(dfQuarterFilter.index) > 0 else None
        periodid2_tstamp = dfQuarterFilter.iloc[0]['PERIODID2_TSTAMP'] if len(dfQuarterFilter.index) > 0 else None
        #
        #   Add Year
        #
        dfYearFilter = dataFrameYear[(dataFrameYear['START_DATE'] <= searchDate) & (dataFrameYear['END_DATE'] >= searchDate)]
        periodid1 = dfYearFilter.iloc[0]['PERIODID1'] if len(dfYearFilter.index) > 0 else None
        periodid1_tstamp = dfYearFilter.iloc[0]['PERIODID1_TSTAMP'] if len(dfYearFilter.index) > 0 else None

        rowDate = None
        if (timePeriodLevel == 1 and periodid1_tstamp != None):
            rowDate = datetime(periodid1_tstamp.year, periodid1_tstamp.month, periodid1_tstamp.day)
        if (timePeriodLevel == 2 and periodid2_tstamp != None):
            rowDate = datetime(periodid2_tstamp.year, periodid2_tstamp.month, periodid2_tstamp.day)
        if (timePeriodLevel == 3 and periodid3_tstamp != None):
            rowDate = datetime(periodid3_tstamp.year, periodid3_tstamp.month, periodid3_tstamp.day)
        if (timePeriodLevel == 4 and periodid4_tstamp != None):
            rowDate = datetime(periodid4_tstamp.year, periodid4_tstamp.month, periodid4_tstamp.day)
        if (timePeriodLevel == 5):
            rowDate = searchDate

        #
        #   If extracting time period level 4, make sure that a week is included only once
        #
        if (timePeriodLevel != 4 or rowDate not in addedWeeks):
            dataList.append({'PERIODID5_TSTAMP':row['PERIODID5_TSTAMP'],
                            'PERIODID4_TSTAMP':periodid4_tstamp,
                            'PERIODID3_TSTAMP':periodid3_tstamp,
                            'PERIODID2_TSTAMP':periodid2_tstamp,
                            'PERIODID1_TSTAMP':periodid1_tstamp,
                            'WEEK_TECHNICAL':row['PERIODID5'],
                            'WEEK':periodid4,
                            'MONTH':periodid3,
                            'QUARTER':periodid2,
                            'YEAR':periodid1,
                            'DATE':rowDate})
            addedWeeks.add(rowDate)
        
    df = pandas.DataFrame(dataList)

    #
    #   Keep only columns required for requested period level
    #
    if (timePeriodLevel < 5):
        df = df.drop(columns=['WEEK_TECHNICAL'])
    if (timePeriodLevel < 4):
        df = df.drop(columns=['WEEK'])
    if (timePeriodLevel < 3):
        df = df.drop(columns=['MONTH'])
    if (timePeriodLevel < 2):
        df = df.drop(columns=['QUARTER'])
    if (timePeriodLevel < 1):
        df = df.drop(columns=['YEAR'])

    if (timePeriodLevel != 5):
        df = df.drop(columns=['PERIODID5_TSTAMP'])
    if (timePeriodLevel != 4):
        df = df.drop(columns=['PERIODID4_TSTAMP'])
    if (timePeriodLevel != 3):
        df = df.drop(columns=['PERIODID3_TSTAMP'])
    if (timePeriodLevel != 2):
        df = df.drop(columns=['PERIODID2_TSTAMP'])
    if (timePeriodLevel != 1):
        df = df.drop(columns=['PERIODID1_TSTAMP'])

    return df.drop_duplicates()

#
#   Generator function that returns one page of data at a time, each page is a list
#   of records (implemented as dictionnaries)
#
def _RequestIbpDataByPage(entitySet, columnList, keyFigureList, renamedColumnList, filter, uom, currency, pageSize, excludeAllZeroesLines):
    #
    #   Build the select clause
    #
    selectValue = ",".join(columnName for columnName in columnList)
    print(f"Select: {selectValue}")
    request = entitySet.get_entities().select(selectValue)
    #
    #   Build the filter clause
    #    
    filterValue = ""
    if (filter != None):
        #
        #   Add custom filter
        #
        filterValue = f"({filter})"
    
    if (uom != None):
        #
        #   Add UOM filter
        #
        if (filterValue != ""):
            filterValue += " and "
        filterValue += f"UOMTOID eq '{uom}' and UOMID eq '{uom}'"

    if (currency != None):
        #
        #   Add Currency filter
        #
        if (filterValue != ""):
            filterValue += " and "
        filterValue += f"CURRTOID eq '{currency}' and CURRID eq '{currency}'"

    if (len(keyFigureList) > 0 and excludeAllZeroesLines):
        #
        #   Add filter to exclude lines with key figures that are all zero
        #
        if (filterValue != ""):
            filterValue += " and "
        filterValue += "("
        firstKeyFigure = True
        for keyFigure in keyFigureList:
            if (not firstKeyFigure):
                filterValue += " or "
            filterValue += keyFigure + " gt 0"
            firstKeyFigure = False 
        filterValue += ")"

    print(f"Filter: {filterValue}")
    request = request.filter(filterValue)

    #
    #   Sets the page size for the request
    #
    request = request.top(pageSize)
    #
    #   We request data from IBP as long as we have remaining data to retrieve
    #
    dataRemaining = True
    pageNumber = 1
    while dataRemaining:
        rowCount = 0
        data = []
        #
        #   We skip the data from the previous pages
        #
        request = request.skip(pageSize * (pageNumber - 1))
        try:
            print(f"Retrieving data ({str(uom)}/{str(currency)}) page : {str(pageNumber)}... ({time.strftime('%H:%M:%S', time.localtime())})")
            #
            #   Retrieve data from IBP for the current page
            #
            result = request.execute()
            print(f"Execute finished ({str(uom)}/{str(currency)}) page : {str(pageNumber)}... ({time.strftime('%H:%M:%S', time.localtime())})")
            #
            #   For each retrieved row, we append the row to the data list
            #
            for row in result:
                rowData = {}
                for columnName in columnList:
                    if (columnName in keyFigureList):
                        rowData[renamedColumnList[columnName] if columnName in renamedColumnList else columnName] = float(row._cache[columnName])
                    else:
                        rowData[renamedColumnList[columnName] if columnName in renamedColumnList else columnName] = row._cache[columnName]
                data.append(rowData)
                rowCount += 1
            print(f"Data appended ({str(uom)}/{str(currency)}) page : {str(pageNumber)}... ({time.strftime('%H:%M:%S', time.localtime())})")
            #
            #   Yield data page
            #
            yield data
            #
            #   Check if we possibly have remaining data
            #   If the last page was not full, we are certain not to have any remaining data
            #
            if (rowCount < pageSize):
                dataRemaining = False
            pageNumber += 1
        except Exception as ex:
                print(ex.response.content)
                exit()


#
#   Returns the segmentation list
#
def _GetSegmentList(entitySet, segmentationColumn):

    segmentList = []
    if segmentationColumn == None:
        #
        #   If segmentation was not requested, we have only one segment with value None
        #
        segmentList.append(None)
    else:
        #
        #   Retrieve list of segmentation column values
        #
        requestSegment = entitySet.get_entities().select(segmentationColumn)
        try:
            resultSegment = requestSegment.execute()
        except Exception as ex:
            print(ex.response.content)
            exit()
        print(f"Extracted segments list... ({time.strftime('%H:%M:%S', time.localtime())})")
        #
        #   Build segments list
        #
        for row in resultSegment:
            segmentList.append(row._cache[segmentationColumn])

    return segmentList

#
#   Generates the DataFrame from the data returned by the oData
#
def _GenerateDataFrame(data, dfTimePeriod = None, tstampColumn = None, removeTstampColumn = None):
        
    dataFrame = pandas.DataFrame(data)
    if tstampColumn != None:
        dataFrame = dataFrame.merge(right=dfTimePeriod, how="left", on=tstampColumn)
    if removeTstampColumn != None and removeTstampColumn:
        dataFrame = dataFrame.drop(tstampColumn, axis='columns')
    print(f"Data Frame generated ({time.strftime('%H:%M:%S', time.localtime())})")
    print(dataFrame)

    return dataFrame


#
#   Extracts data from IBP and puts result in a pandas DataFrame object. Generator version.
#
#   Parameters :
#       entitySet               : entity set object used to retrieve data
#                                   Mandatory
#       columnList              : list of columns to retrieve, including key figures (measures)
#                                   Mandatory
#                                   Example : ['PRDID','LOCID','CUSTID','UOMID','ZSPOPENORDERSQTY']
#       keyFigureList           : list of columns which are key figures (measures)
#                                   Optional
#                                   Default value : empty list
#                                   Example : ['ZSPOPENORDERSQTY']
#       isCurrency              : indicates if the key figures are currency amounts instead of quantities (True or False)
#                                   Default value : False
#       renamedColumnList       : dictionnary of columns to be renamed in the result (Key : column name, Value : new name)
#                                   Optional
#                                   Default value : empty dictionnary
#                                   Example : {"UOMID":"UOM", "ZSPOPENORDERSQTY": "OPEN ORDERS QTY"} 
#                                               (column UOMID is renamed UOM in the result, and ZSPOPENORDERSQTY is renamed OPEN ORDERS QTY)
#       filter                  : custom filter
#                                   Optional
#                                   Default value : None
#                                   Example : "PLUNITID eq 'CTG' and PERIODID5_TSTAMP ge datetime'2022-11-01T00:00:00'"
#       pageSize                : size of pages used to retrieve data. Paging is used to avoir errors due to data size
#                                   Optional
#                                   Default value : 100000
#       segmentSize             : maximum size of segments (number of rows).
#                                      If set to None or a value less tahn or equal to zero, segment size will not be limited
#                                       Note : the size of resulting DataFrames can sligthy exceed the maximum depending
#                                               on how much data is retrieved in each page
#                                   Optional
#                                   Default value : 1000000
#       excludeAllZeroesLines   : True or False; if True, lines for which all key figures are zero will be excluded
#                                   Optional
#                                   Default value : True
#
#       timePeriodLevel         : 1 to 5; defines the time period level for extraction of key figure
#                                   Default value : 5
#                                   1 : YEAR
#                                   2 : QUARTER
#                                   3 : MONTH
#                                   4 : WEEK
#                                   5 : WEEK_TECHNICAL
#
#       segmentationColumn      : name of column used to segment the result. For each unqiue value of that column a DataFrame
#                                       will be generated.
#                                       If specified, the function will return a list of DataFrames, if not specified it returns a DatatFrame
#                                   Default value : None
#
#       NOTES :
#
#           filter: the function will build a filter clause to be used in the extract. The filter clause will first include
#                   the custom filter passed as a parameter to the function. The following additional filters may also be added
#
#               uom filter :            if key figures (quantities) are requested, a filter will be added to get the base unit of measure.
#                                       Example for CS : UOMTOID eq 'CS' and UOMID eq 'CS'
# 
#               currency filter :       if key figures (currencies) are requested, a filter will be added to get currency id.
#                                       Example for CS : CURRTOID eq 'CAD' and CURRID eq 'CAD'
# 
#               key figures filter :    if key figures are requested and the parameter excludeAllZeroesLines is set to True
#                                       then a filter is added to exclude lines where all key figures are zero
#                                       Example : (ZSPOPENORDERSQTY gt 0 or ZSPOPENORDERSWOZINVQTY gt 0)
#
#           uom :   if key figures (quantities) are included, the function will retrieve the list of units of measure from the source and then
#                   run one data request for each uom and combine them together in one result
#
#           currency :  if key figures (currencies) are included, the function will retrieve the list of currencies from the source and then
#                       un one data request for each currency and combine them together in one result
#
#           time periods :  if at least one key figure is requested, column PERIODID5_TSTAMP is used to do a join
#                           with the period 1 to 5 extraction. If the column is not present in columnList, it is automticaly
#                           added to the list.
#                           The column will be removed from the result if it was not initially present.
#
def ExtractIbpDataGen(entitySet, 
                   columnList, 
                   keyFigureList = [], 
                   isCurrency = False,
                   renamedColumnList = {}, 
                   filter = None, 
                   pageSize = 100000,
                   segmentSize = 1000000, 
                   excludeAllZeroesLines = True,
                   timePeriodLevel = 5,
                   segmentationColumn = None):

    print(f"Extracting data... ({time.strftime('%H:%M:%S', time.localtime())})")

    print("Parameters:")
    print(f"columnList           : {columnList}")
    print(f"keyFigureList        : {keyFigureList}")
    print(f"isCurrency           : {isCurrency}")
    print(f"renamedColumnList    : {renamedColumnList}")
    print(f"filter               : {filter}")
    print(f"pageSize             : {pageSize}")
    print(f"excludeAllZeroesLines: {excludeAllZeroesLines}")
    print(f"timePeriodLevel      : {timePeriodLevel}")

    if (len(keyFigureList) > 0):

        dfTimePeriod = _ExtractIbpPeriod(entitySet, timePeriodLevel)

        #   If we have key figures (measures) we need to add PERIODID5_TSTAMP 
        #   to the list of columns if it is not present
        #   The column is used to do a join with the periods extraction and is the removed
        #   from the result
        removeTstampColumn = False
        tstampColumn = f"PERIODID{timePeriodLevel}_TSTAMP"
        if not(tstampColumn in columnList):
            columnList.append(tstampColumn)
            removeTstampColumn = True

        segmentList = _GetSegmentList(entitySet, segmentationColumn)

        print("Segmentation column values:")
        print(segmentList)

        #
        #   Generate a DataFrame for each segment
        #
        #   When we don't have a segmentation column, we will have only one segment with a value of None
        #
        for segment in segmentList:

            data = []
            #
            #   Add the segment filter if segment is not None
            #
            segmentFilter = ''
            if (filter != None):
                segmentFilter = "(" + filter + ")"
            if segment != None:
                if (segmentFilter != ""):
                    segmentFilter += " and "
                segmentFilter += f"({segmentationColumn} eq '{segment}')"
            if segmentFilter == '':
                segmentFilter = None

            #
            #   Generate the list of currencies or uoms
            #
            requestCurrUom = entitySet.get_entities().select('CURRID' if isCurrency else 'UOMID')
            try:
                resultCurrUom = requestCurrUom.execute()
            except Exception as ex:
                print(ex.response.content)
                exit()
            print(f"Extracted Currencies/UOMs... ({time.strftime('%H:%M:%S', time.localtime())})")

            #
            #   For each currency or uom, request data
            #
            for currUom in resultCurrUom:
                #
                #   Loop through each data page returned
                #
                for page in _RequestIbpDataByPage(entitySet = entitySet,
                                                    columnList = columnList,
                                                    keyFigureList = keyFigureList,
                                                    renamedColumnList = renamedColumnList,
                                                    filter = segmentFilter,
                                                    uom = None if isCurrency else currUom.UOMID,
                                                    currency = currUom.CURRID if isCurrency else None,
                                                    pageSize = pageSize,
                                                    excludeAllZeroesLines = excludeAllZeroesLines):
                    #
                    #   Append the content of the page to the data
                    #
                    for row in page:
                        data.append(row)
                    #
                    #   If we have reached the segment size, we yield a DataFrame and clear the data
                    #
                    if segmentSize != None and segmentSize > 0 and len(data) >= segmentSize:
                        dataFrame = _GenerateDataFrame(data, dfTimePeriod, tstampColumn, removeTstampColumn)
                        yield dataFrame
                        data = []

            #
            #   When finished reading the data for the segment, we need to make sure to generate
            #   a DataFrame for any remaining data
            #
            if len(data) > 0:
                dataFrame = _GenerateDataFrame(data, dfTimePeriod, tstampColumn, removeTstampColumn)
                yield dataFrame 

    else:

        print(f"Generating result Data Frame... ({time.strftime('%H:%M:%S', time.localtime())})")
        data = []
        #
        #   If no key figures, we have no uom to specify
        #
        for page in _RequestIbpDataByPage(entitySet = entitySet,
                                          columnList = columnList, 
                                          keyFigureList = keyFigureList,
                                          renamedColumnList = renamedColumnList,
                                          filter = filter,
                                          uom = None,
                                          currency = None,
                                          pageSize = pageSize,
                                          excludeAllZeroesLines = excludeAllZeroesLines):
            for row in page:
                data.append(row)

            #
            #   If we have reached the segment size, we yield a DataFrame and clear the data
            #
            if segmentSize != None and segmentSize > 0 and len(data) >= segmentSize:
                dataFrame = _GenerateDataFrame(data)
                yield dataFrame
                data = []

        #
        #   When reading all the pages is done, if we have data remaining we yield a DataFrame
        #
        if len(data) > 0:
            dataFrame = _GenerateDataFrame(data)
            yield dataFrame

def create_session(SERVICE_URL,USER_ID,PASS):
    import requests
    import pyodata
    SERVICE_URL = SERVICE_URL
    session = requests.Session()
    USER=USER_ID
    PASSWORD=PASS
    session.auth = (USER, PASSWORD)
    entityset = pyodata.Client(SERVICE_URL, session).entity_sets.ENTITY #Make sure to replace ENTITY with the correct ENTITY of your organization

    return entityset
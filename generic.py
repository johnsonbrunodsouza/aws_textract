
import pandas as pd
import json

# Constants - JSON identifiers
TXTR_BLOCKS='Blocks'
TXTR_BLOCKTYPE='BlockType'
TXTR_BOUNDINGBOX='BoundingBox'
TXTR_CELL='CELL'
TXTR_CHILD='CHILD'
TXTR_COLUMNINDEX='ColumnIndex'
TXTR_CONFIDENCE='Confidence'
TXTR_DOCUMENT='Document'
TXTR_ENTITY_TYPES='EntityTypes'
TXTR_EXTRACTEDTEXT='ExtractedText'
TXTR_GEOMERTY='Geometry'
TXTR_HEIGHT='Height'
TXTR_ID='Id'
TXTR_IDS='Ids'
TXTR_KEY_VALUE_SET='KEY_VALUE_SET'
TXTR_KEY='KEY'
TXTR_LEFT='Left'
TXTR_LINE='LINE'
TXTR_PAGE='Page'
TXTR_RELATIONSHIPS='Relationships'
TXTR_ROWINDEX='RowIndex'
TXTR_SELECTION_ELEMENT='SELECTION_ELEMENT'
TXTR_SELECTION_STATUS='SelectionStatus'
TXTR_TABLE='TABLE'
TXTR_TEXT='Text'
TXTR_TOP='Top'
TXTR_TYPE='Type'
TXTR_VALUE='VALUE'
TXTR_WIDTH='Width'

# Constants for configuration file
CONF_BUSINESSFIELD='BusinessField'
CONF_CONFIGURATIONS='Configurations'
CONF_END_TEXT='EndText'
CONF_ENDFIELD='EndField'
CONF_EXTRACT_BETWEEN_LAST_EXTRACTED_FIELD_AND_END_FIELD='between_last_extracted_field_and_end_field'
CONF_EXTRACT_NEXT_FIELD='next_field'
CONF_EXTRACT_END_FIELD='end_field'
CONF_EXTRACTTYPE='ExtractType'
CONF_FIELDS_TO_EXTRACT='FieldsToExtract'
CONF_JSONFIELD='JSONField'
CONF_PDFFIELD='PDFField'
CONF_ROW_HEADER_SEPARATOR='ROW_HEADER_SEPARATOR'
CONF_SECTION='Section'
CONF_START_TEXT='StartText'
CONF_TABLE_CONFIGURATIONS='TableConfigurations'
CONF_TABLE_HEADER='TABLE_HEADER'
CONF_TEXTTOREMOVE='TextToRemove'
CONF_EXTRACT_TYPE='Type'
CONF_EXTRACT_TYPE_REGULAR_TEXT='Regular Text'
CONF_EXTRACT_TYPE_FORM_TEXT='Form Text'
CONF_EXTRACT_TYPE_REPEATABLE_SECTIONS='Repeatable Sections'
CONF_OPTION_FIELDS='option_fields'



def getBlockContents(block):
    if(block.get(TXTR_BLOCKTYPE)!=None and
        block.get(TXTR_TEXT)!=None and
        block.get(TXTR_CONFIDENCE)!=None and
        block.get(TXTR_PAGE)!=None and
        block.get(TXTR_GEOMERTY)!=None):

        blockType=block.get(TXTR_BLOCKTYPE)
        text=block.get(TXTR_TEXT)
        conf=float(block.get(TXTR_CONFIDENCE))
        page=int(block.get(TXTR_PAGE))
        geometry={TXTR_WIDTH:float(block.get(TXTR_GEOMERTY).get(TXTR_BOUNDINGBOX).get(TXTR_WIDTH)),
        TXTR_HEIGHT:float(block.get(TXTR_GEOMERTY).get(TXTR_BOUNDINGBOX).get(TXTR_HEIGHT)),
        TXTR_LEFT:float(block.get(TXTR_GEOMERTY).get(TXTR_BOUNDINGBOX).get(TXTR_LEFT)),
        TXTR_TOP:float(block.get(TXTR_GEOMERTY).get(TXTR_BOUNDINGBOX).get(TXTR_TOP))
        }

        return {TXTR_BLOCKTYPE:blockType,TXTR_TEXT:text,TXTR_CONFIDENCE:conf,TXTR_PAGE:page,TXTR_GEOMERTY:geometry}
    else:
        return None

def getTextBetweenBlocks(startText,endText,blocks):
    filteredText=[]
    blockFlag=False
    endFlag=False
    index=0
    blockCounter=0
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]
            blockMemebers = getBlockContents(block)

            if blockMemebers != None and blockMemebers[TXTR_TEXT] == startText:
                blockFlag=True
            if blockMemebers != None and blockMemebers[TXTR_TEXT] == endText:
                blockFlag=False
                endFlag=True
            
            if blockMemebers !=None and blockFlag and blockMemebers[TXTR_TEXT]!=startText and blockMemebers[TXTR_BLOCKTYPE] == TXTR_LINE:
                filteredText.append(blockMemebers[TXTR_TEXT])
            if endFlag:
                break
            index+=1
        blockCounter+=1
    return {startText:filteredText}

def isRowFilled(row,mandatoryFieldsPercent=100):
    flag=False
    count=0
    for key in row:
        if row[key]!=None:
            count+=1

    if count>len(row)*mandatoryFieldsPercent/100:
        flag=True
    return flag


def getTableIDs(blocks):
    tableIDs={}

    blockCounter=0
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]
            blockMemebers = getBlockContents(block)
            blockType=block.get(TXTR_BLOCKTYPE)

            if blockMemebers != None and blockType==TXTR_TABLE:
                tableId=block.get(TXTR_ID)
                childIds=block.get(TXTR_RELATIONSHIPS)[0].get(TXTR_IDS)
                tableIDs[tableId]=childIds
            index+=1
        blockCounter+=1
    return tableIDs

def getTable(tableIds,blocks,headerRows,rowHeaderSeparator,columnLength,configHeader):
    table=[]
    columnCount=1
    row=[]
    headers=[]

    isFirstValue=True
    rowHeaderCount=headerRows

    for item in tableIds:
        value=getCellContents(item,blocks)

        if columnCount<=columnLength:
            if isFirstValue:
                if value[TXTR_TEXT]!=configHeader[0]:
                    return pd.DataFrame()
                isFirstValue=False
            row.append(value[TXTR_TEXT])
            columnCount=columnCount+1
        else:
            if rowHeaderCount>0:
                headers.append(row)
                rowHeaderCount-=1
            else:
                table.append(row)

                row=[]
                row.append(value[TXTR_TEXT])
                columnCount=2
    table.append(row)

    pdDf=pd.DataFrame()
    isValidHeader=True
    mergedHeader=headers

    if headerRows>len(headers) or len(headers)>0 and len(table[0])!=len(headers[0]):
        isValidHeader=False
    else:
        mergedHeader=[]

        for rowIndex in range(0,headerRows):
            for colIndex in range(0,columnLength):

                if colIndex==len(headers[rowIndex]):
                    isValidHeader=False
                    break
                else:
                    if rowIndex==0:
                        mergedHeader.append(str(headers[0][colIndex]))
                    else:
                        if headers[rowIndex][colIndex]!='':
                            mergedHeader[colIndex]=mergedHeader[colIndex]+rowHeaderSeparator+str(headers[rowIndex][colIndex])
            if not isValidHeader:
                break
    if isValidHeader:
        pdDf=pd.DataFrame(table,columns=mergedHeader)
    return pdDf


def getCellContents(blockId,blocks,separator=' '):
    blockCounter=0
    text=''
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]
            
            if block.get(TXTR_ID)==blockId:
                if block.get(TXTR_RELATIONSHIPS)==None:
                    text=''
                else:
                    relationships=block.get(TXTR_RELATIONSHIPS)[0]
                    childIds=relationships.get(TXTR_IDS)
                    text=getIDText(childIds,blocks,separator)
            index+=1
        blockCounter+=1
    return {TXTR_TEXT:text}

def getIDText(blockIds,blocks,separator):
    text=''
    for item in blockIds:
        blockCounter=0
        index=0
        while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
            index=0
            while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
                block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]
                if block.get(TXTR_ID)==item and TXTR_TEXT in block:
                    if text=='':
                        text.block.get(TXTR_TEXT)
                    else:
                        text=text+separator+text.block.get(TXTR_TEXT)
                index+=1
            blockCounter+=1
    return text

def mergeTables(tableList):
    rows=[]
    for tab in tableList:
        for r in tab.values.tolist():
            rows.append(r)
    
    if len(rows)==0:
        return pd.DataFrame()

    rIndex=0
    while rIndex<len(rows)-1:
        cIndex=0
        while cIndex<len(rows[rIndex]):
            if rows[rIndex][cIndex]!='':
                if len(rows[rIndex][cIndex])>1 and rows[rIndex][cIndex].endswith('-'):
                    rows[rIndex][cIndex]=rows[rIndex][cIndex]+rows[rIndex+1][cIndex]
                    rows[rIndex+1][cIndex]=''
            cIndex+=1
        rIndex+=1
    
    filteredRows=[]
    rIndex=0
    while rIndex<len(rows):
        isEmptyRow=True
        cIndex=0
        while cIndex<len(rows[rIndex]):
            if rows[rIndex][cIndex]!='':
                isEmptyRow=False
                break
            cIndex+=1
        if not isEmptyRow:
            filteredRows.append(rows[rIndex])
        rIndex+=1
    return pd.DataFrame(filteredRows,columns=tableList[0].columns)

def setExtractRow(fieldsExtractionConfig):
    row={}
    for item in fieldsExtractionConfig:
        row[item[CONF_JSONFIELD]]=None
    return row

def getNextToken(tokens,searchToken,startAt=0):
    searchStringFoundAt=tokens.find(searchToken,startAt)

    if searchStringFoundAt>0:
        endOfNextToken=tokens.find(' ',searchStringFoundAt+len(searchToken)+1)
        nextToken=tokens[searchStringFoundAt+len(searchToken):endOfNextToken]
        index=endOfNextToken
        return nextToken,index
    else:
        return None,startAt

def getTextBetweenTokens(tokens,searchTokenStart,searchTokenEnd,startAt,removeText):
    searchTokenStartFoundAt=tokens.find(searchTokenStart,startAt)
    searchTokenEndFoundAt=tokens.find(searchTokenEnd,startAt)

    if searchTokenStartFoundAt>=0 and searchTokenEndFoundAt>searchTokenStartFoundAt:
        textBetween=tokens[searchTokenStartFoundAt+len(searchTokenStart):searchTokenEndFoundAt]
        for text in removeText:
            textBetween=textBetween.replace(text,'')

        index=searchTokenEndFoundAt
        return textBetween.strip(),index
    else:
        return None,startAt

def getTextForConfigV3(textLst,fieldsExtractionConfig,mandatoryFieldsPercentage=100):
    row=setExtractRow(fieldsExtractionConfig)
    tokens=''
    resultList=[]

    for item in textLst:
        for i in item.split(' '):
            tokens=tokens+' '+i
    tokens=tokens.strip()

    index=0

    lastValueExtracted=''
    firstOccurance=''
    firstOccuranceIndex=0
    while index<len(tokens):
        for item in fieldsExtractionConfig:
            if item[CONF_EXTRACTTYPE]==CONF_EXTRACT_NEXT_FIELD:
                for pdfField in item[CONF_PDFFIELD]:
                    valueFound,valueFoundAt=getNextToken(tokens,pdfField,index)
                    if valueFound!=None and valueFoundAt!=index:
                        if firstOccuranceIndex==0 and firstOccurance=='':
                            firstOccurance=valueFound
                            firstOccuranceIndex=valueFoundAt
                        else:
                            if valueFoundAt<firstOccuranceIndex:
                                firstOccurance=valueFound
                                firstOccuranceIndex=valueFoundAt

                if firstOccuranceIndex==0 and firstOccurance=='':
                    row[item[CONF_JSONFIELD]]=None
                    index+=1
                else:
                    row[item[CONF_JSONFIELD]]=firstOccurance.strip()
                    index=firstOccuranceIndex
                    lastValueExtracted=firstOccurance

                firstOccurance=''
                firstOccuranceIndex=0
            elif item[CONF_EXTRACTTYPE]==CONF_EXTRACT_BETWEEN_LAST_EXTRACTED_FIELD_AND_END_FIELD:
                valuesFoundList=[]
                for endText in item[CONF_ENDFIELD]:
                    valueFound,valueFoundAt=getNextToken(tokens,endText,index)
                    valueFound,valueFoundAt=getTextBetweenTokens(tokens,lastValueExtracted,endText,index-len(lastValueExtracted),item[CONF_TEXTTOREMOVE])

                    if valueFound!=None and valueFoundAt!=index:
                        valuesFoundList.append([valueFound,valueFoundAt])
                if len(valuesFoundList)>0:
                    firstOccurance,firstOccuranceIndex=getFirstFoundText(valuesFoundList)
                    row[item[CONF_JSONFIELD]]=firstOccurance
                    index=firstOccuranceIndex
                    lastValueExtracted=firstOccurance
                else:
                    row[item[CONF_JSONFIELD]]=None
                    index+=1

                    firstOccurance=''
                    firstOccuranceIndex=''
            elif item[CONF_EXTRACTTYPE]==CONF_EXTRACT_END_FIELD:
                valueFoundList=[]
                for endText  in item[CONF_ENDFIELD]:
                    valueFound,valueFoundAt=getTextBetweenTokens(tokens,item[CONF_PDFFIELD][0],endText,index-len(lastValueExtracted),item[CONF_TEXTTOREMOVE])
                    if valueFound!=None and valueFoundAt!=index:
                        valueFoundList.append([valueFound,valueFoundAt])

                    if len(valueFoundList)>0:
                        firstOccurance,firstOccuranceIndex=getFirstFoundText(valuesFoundList)
                        row[item[CONF_JSONFIELD]]=firstOccurance
                        index=firstOccuranceIndex-1
                        lastValueExtracted=firstOccurance
                        firstOccurance=''
                        firstOccuranceIndex=0
                    else:
                        row[item[CONF_JSONFIELD]]=None
                        index+=1
                
        if  isRowFilled(row):
            resultList.append(row)
            row=setExtractRow(fieldsExtractionConfig)

    return resultList


def getFormTextForConfigV3(textLst,fieldsExtractionConfig):
    row=setExtractRow(fieldsExtractionConfig)
    resultList=[]

    index=0
    while index<len(textLst)-1:
        for item in fieldsExtractionConfig:
            if  item[CONF_EXTRACTTYPE]==CONF_EXTRACT_NEXT_FIELD:
                for pdfField in item[CONF_PDFFIELD]:
                    foundValue=False
                    while index<len(textLst)-1 and foundValue==False:
                        if textLst[index]==pdfField:
                            foundValue=TXTR_VALUE
                            row[item[CONF_JSONFIELD]]=textLst[index+1]
                        index+=1
        
        if isRowFilled(row):
            resultList.append(row)
            row=setExtractRow(fieldsExtractionConfig)
    return resultList

def getFirstFoundText(valuesFoundLst):
    text_textFoundAt=[None,-1]

    if len(valuesFoundLst)==1:
        text_textFoundAt=valuesFoundLst[0]
    elif len(valuesFoundLst)>1:
        index=1
        firstOccurance=valuesFoundLst[0][0]
        firstOccuranceIndex=valuesFoundLst[0][1]

        while index<len(valuesFoundLst):
            if firstOccuranceIndex>valuesFoundLst[index][1]:
                firstOccuranceIndex=valuesFoundLst[index][1]
                firstOccurance=valuesFoundLst[index][0]
            index+=1
        text_textFoundAt[0]=firstOccurance.strip()
        text_textFoundAt[1]=firstOccuranceIndex
    
    return text_textFoundAt[0],text_textFoundAt[1]

def getSelectionElementMap(blocks):
    selectionMap={}
    blockCounter=0

    blockCounter=0
    index=0
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]

            if block[TXTR_BLOCKTYPE]==TXTR_SELECTION_ELEMENT:
                selectionMap[block[TXTR_ID]]=block[TXTR_SELECTION_STATUS]
            index+=1
        blockCounter+=1
    return selectionMap

def getKeyValueSetIDs(blocks):
    keyValueSetIdList=[]
    
    blockCounter=0
    index=0
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]
            blockType=block.get(TXTR_BLOCKTYPE)
            if blockType!=None and blockType==TXTR_KEY_VALUE_SET and block[TXTR_ENTITY_TYPES][0]==TXTR_KEY:
                keyValueSetIdList.append(block[TXTR_ID])
            index+=1
        blockCounter+=1
    return keyValueSetIdList

def getKeyValueSetIDs(setId,blocks):
    blockCounter=0
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]

            if block[TXTR_ID]==setId and len(block[TXTR_RELATIONSHIPS])==2:
                keyIds=block[TXTR_RELATIONSHIPS][1][TXTR_IDS]
                valueId=block[TXTR_RELATIONSHIPS][0][TXTR_IDS][0]
                valueIdLst=getChildRelationships(valueId,blocks)
                keyText=getIDText(keyIds,blocks,' ')
                valueText=getIDText(valueIdLst,block,' ')
                if  valueText=='':
                    valueText=getSelectionElementMap(valueIdLst,blocks)
                return keyText,valueText,keyIds
            index+=1
        blockCounter+=1
    return '','',[]

def getChildRelationships(searchId,blocks):
    blockCounter=0
    index=0
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]
            if  block(TXTR_ID)==searchId and TXTR_RELATIONSHIPS in block:
                return block[TXTR_SELECTION_STATUS]
            index+=1
        blockCounter+=1
    return None


def getSelectionValue(searchIdLst,blocks):
    blockCounter=0
    index=0
    while blockCounter<len(blocks[TXTR_EXTRACTEDTEXT]):
        index=0
        while index<len(blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKTYPE]):
            block=blocks[TXTR_EXTRACTEDTEXT][blockCounter][TXTR_BLOCKS][index]
            if  block(TXTR_ID) in searchIdLst and block[TXTR_BLOCKTYPE]==TXTR_SELECTION_ELEMENT:
                return block[TXTR_SELECTION_STATUS]
            index+=1
        blockCounter+=1
    return None

def setConfigItem(businessField,pdfField,extractType,jsonField,endField=[],textToRemove=[],optionFields=[]):
    conf={}
    conf[CONF_BUSINESSFIELD]=businessField
    conf[CONF_PDFFIELD]=pdfField
    conf[CONF_EXTRACTTYPE]=extractType
    conf[CONF_JSONFIELD]=jsonField

    if len(endField)>0:
        conf[CONF_ENDFIELD]=endField
    conf[CONF_TEXTTOREMOVE]=textToRemove

    if len(optionFields)>0:
        conf[CONF_OPTION_FIELDS]=optionFields
    return conf

if __name__ == "__main__":
    with open('SampleFile-Name.json') as responseFile:
        jsonBlocks=json.load(responseFile)
        print(jsonBlocks)

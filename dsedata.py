# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Purpose: This script automatically fetch metadata from DataMart for a DSE datalayer stored in CSDILA metadatatool system. 
# Version: 1.0
# Author: Benny Chen (yiqun.c@unimelb.edu.au)
# Last Update: 09-July-2013
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

import urllib
import json
from bs4 import BeautifulSoup
import psycopg2
from time import strftime, localtime
import csv
import string

def batchUpdate():
	records = csv.reader(open("./DM_URL.csv","rU"))
	counter = 0
	nourlcounter = 0
	failcounter = 0
	for r in records:
		# if the url does not exist, ignore it
		if len(r[3]) < 100 :
			print "Skipped. No valid URL for layer:", r[2].strip()
			nourlcounter=nourlcounter+1
			continue
		
		# find the url
		url = r[3]
		layerName = r[2].strip()
		print "Processing layer :", r[2].strip()
		if updateModifiableContentByLayerName(url,layerName):
			counter = counter + 1
		else:
			failcounter = failcounter + 1
		
	print "handled:", counter, " nourl:",nourlcounter, " failed:", failcounter 

def updateModifiableContentByLayerName(url, layerName):	
	"""update data_aurin_modifiable column value for a target layer with its datamart url """
	flag = False
	#dev db PLACE1/2
	#conn = psycopg2.connect(database="postgres25", user="postgres", password="******", host="XXXXXX", port="5432")
	#product db PLACE1/2
	#conn = psycopg2.connect(database="postgres13", user="postgres", password="******", host="XXXXXX", port="5432")
	cur = conn.cursor()
	cur.execute("Select id, data_aurin_modifiable from metadata where upper(name)=%s", [layerName.upper()])
	record = cur.fetchone()
	if record is not None:
		content = record[1]
		id = record[0]
		soup = enrichAttrMetadata(url, content)
		if soup is not None:
			updatedContent = str(soup)[39:].replace("'", "’").replace("'", "”").replace("\\r\\n"," ")
			updatedTime = strftime("%Y-%m-%dT%H:%M:%S",localtime())
			cur.execute("update metadata set data_aurin_modifiable=%s, retrievedate=%s where id=%s", [updatedContent, updatedTime,id])
			conn.commit()
			flag = True
		else:
			print "Skipped. cannot load valid metadata content from URL: ", url
	else:
		print "Skipped. cannot find layer : ", layerName
		
	cur.close()
	conn.close()
	return(flag)

def getModifiableContentByLayerName(layerName):	
	"""get data_aurin_modifiable column value (string) for a target layer """
	#dev db PLACE2/2
	#conn = psycopg2.connect(database="postgres25", user="postgres", password="******", host="XXXXXX", port="5432")
	#product db PLACE2/2
	conn = psycopg2.connect(database="postgres13", user="postgres", password="******", host="XXXXXX", port="5432")
	cur = conn.cursor()
	cur.execute("Select id, data_aurin_modifiable from metadata where upper(name)=%s", [layerName.upper()])
	record = cur.fetchone()
	if record is not None:
		content = record[1]
		
	cur.close()
	conn.close()
	return(content)

def enrichAttrMetadata(tarLayerUrl, modContent):
	"""construct and return an enriched data_aurin_modifiable beautifulsoup object"""
	#replace unnecessary tag
	modContent = modContent.replace('xmlns:geonet="http://www.fao.org/geonetwork"','')
	
	#try to load metadata from datamart url
	metadataTuple = getAttrMetaData(tarLayerUrl)
	attrMetadataTable = metadataTuple[0]
	
	# if cannot find valid metadata, return modContent
	if (attrMetadataTable is None or len(attrMetadataTable)==0):
		return(None)

	# make soup object
	soupModContent = BeautifulSoup(modContent, "xml")
	
	# update layer abstract
	soupModContent.abstract.CharacterString.string = metadataTuple[1]
	
	# update layer keywords
	soupModContent.keyword.CharacterString.string = metadataTuple[2]
	
	# update dataset availability to true
	if soupModContent.MD_Metadata.MD_AurinAdditions.availability is not None:
		soupModContent.MD_Metadata.MD_AurinAdditions.availability.decompose()
	avaTag = soupModContent.new_tag("aurin:availability")
	avaTag.string = "true"
	soupModContent.MD_Metadata.MD_AurinAdditions.append(avaTag)
	
	# get datasetattributename tag list from soupModContent
	danlist = soupModContent.MD_Metadata.MD_AurinAdditions.find_all('attributeName')
	
	# update metadata in danlist
	for i in range(len(attrMetadataTable)):
		attrname = attrMetadataTable[i][0][1:-1]
		for j in range(len(danlist)):
			if danlist[j].string == attrname:
				# remove abstract tag if exists
				if danlist[j].parent.attributeAbstract is not None:
					danlist[j].parent.attributeAbstract.decompose()
				# create abstract tag
				absTag = soupModContent.new_tag("aurin:attributeAbstract")
				absTag.string = attrMetadataTable[i][6][1:-1]
				danlist[j].parent.insert(0, absTag)

				if danlist[j].parent.attributeComments is not None:
					danlist[j].parent.attributeComments.decompose()
				# create comment(title) tag, the value is identical to attribute name
				comTag = soupModContent.new_tag("aurin:attributeComments")
				comTag.string = attrname
				danlist[j].parent.insert(1, comTag)
				
				# make statistical type judgement
				if danlist[j].parent.attributeSType is not None:
					danlist[j].parent.attributeSType.decompose()
				
				# create statisticaltype tag: from data informative point of view: nominal<ordial<interval<ratio
				# it is safe to downgrade an attribute from ratio to interval, the only cost is losing some statistical info. 
				sttTag = soupModContent.new_tag("aurin:attributeSType")
				sttTag.string = "Nominal"
				if attrname.upper() == "UFI" or attrname.upper() == "PFI" or attrname.upper() == "PFI_OLD" or attrname.upper() == "FEATURE_TYPE_CODE" or string.find(attrname.upper(),"CODE") >=0:
					sttTag.string = "Nominal"
				elif danlist[j].parent.attributeType.string == "java.sql.Timestamp" or danlist[j].parent.attributeType.string == "java.lang.Integer" or danlist[j].parent.attributeType.string == "java.lang.Short" or danlist[j].parent.attributeType.string == "java.lang.Long":
					sttTag.string = "Ordinal"
				elif danlist[j].parent.attributeType.string == "java.lang.Double" or danlist[j].parent.attributeType.string == "java.lang.Float":
					sttTag.string = "Interval"
	
				danlist[j].parent.insert(2, sttTag)				
				# break out
				break
	
	return(soupModContent)		
	

def getAttrMetaData(targetUrl):
	"""parsing datamart url into 3 parts: attributes(list), abstract(string), keywords(string). return them all as a tuple"""
	sock = urllib.urlopen(targetUrl) 
	htmlSource = sock.read()                            
	sock.close()
	
	soup = BeautifulSoup(htmlSource)
	
	tab1 = soup.body.table.tr.td.div.contents[3].div.contents[1].contents[1]
	tab2 = soup.body.table.tr.td.div.contents[3].div.contents[3].contents[1]
	tab3 = soup.body.table.tr.td.div.contents[3].div.contents[7].contents[3]
	
	#parsing tab2
	tr2list = tab2.find_all('tr')
	AnzlicId = tr2list[3].span.string
	Abstract = str(tr2list[7].find_all("td")[1])[4:-5]
	Keywords = AnzlicId+", "+str(tr2list[8].find_all("td")[1])[4:-5]
	
	# parsing tab3
	trlist = tab3.find_all('tr')
	trdata = []
		
	#table content
	for tr in trlist[1:]:
		tddata = []
		tdlist = tr.find_all('td')
		for td in tdlist:
			tdtext = td.text.strip()
			if tdtext == '': tdtext = 'NULL'
			
			tdtext = json.dumps(tdtext)
			tddata.append(tdtext)
		trdata.append(tddata)

	return(trdata, Abstract, Keywords)


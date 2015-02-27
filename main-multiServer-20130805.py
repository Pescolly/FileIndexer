#!/usr/bin/python

#########################################################################################
#################			MAIN APPLICATION FOR DB CREATION			#################
#################			ARMEN KARAMIAN 2013							#################
#################			WRITTEN IN PYTHON WITH SQLITE3	V.	0.002	#################
#########################################################################################



import sqlite3, os, time, hashlib, string, shutil, multiprocessing as m, re, xml.etree.ElementTree as et

xmlFileLocation = 'config.xml'
walkPath = []													### global variables for config file
dbLocation = ""


def getPath(prompt):												####### function to get path and normalize		
	sourcePath = str(raw_input(prompt)).rstrip()				
	while os.path.exists(sourcePath) == False:									#see if path exists
		sourcePath = string.replace(sourcePath,'\ ',' ')
		if os.path.exists(sourcePath) == True:
			break
		sourcePath = str(raw_input('Path not valid. Enter a valid path: ')).rstrip()
	return sourcePath

def source():											##### runs when application begins and on command.
	print "Data Sources: "
	for i in walkPath:									# print servers that are in DB entry tuple			
		print i

	while True:
		command = str(raw_input("Add Path to DB? (y/n) ")).rstrip()
		if command == ('y' or 'Y'):									### option to add new path to DB
			newPath = str(getPath("Enter new path: "))				### get path from prompt
			walkPath.append(newPath)								### append to list of sources
		else:
			print "Sources: "
			for i in walkPath:
				print i
			break
		
	while True:
		command = str(raw_input("Remove Path from DB? (y/n) ")).rstrip()
		if command == ('y' or 'Y'):									#### option to remove path from DB
			j = 0
			for i in walkPath:									# print servers that are in source list
				j+=1											# print out number for index (+1)
				print j, i										# present user with list of sources to remove
			try:											# get index from list printed above	
				index = int(raw_input("Enter number of source path to remove: "))-1	#subtract one to correspond with list
			except IndexError:
				print "Not a valid index number"
			except ValueError:
				print "Not a valid index number"
			finally:	
				refName = walkPath[index]					# create refName for later reference
				del walkPath[index]							# remove selected item from list of sources
#				break
#			while True:
#			command = str(raw_input("Remove",refName,"from DB now?"))		### remove actual listings from database
#			if command == ('y' or 'Y'):
#				print "Sourcename: ", refName
				cursor.execute("""DELETE FROM ingestIndex WHERE filename LIKE ?""", ('%'+refName+'%',))			
#			else:
#				pass
		else:
			break
	return walkPath

def createXML(DBLocationUpdate, runSource):						#### CREATE XML INFORMATION ####
		global dbLocation
		global walkPath
		global xmlFileLocation
		config = et.Element("Config")								#	root element
		if DBLocationUpdate == True:
			dbLocation = getPath("Enter path to place DB: ") + "/database.db"				
		dbFileElem = et.SubElement(config,"dbLocation")				#	subelement for db location
		dbFileElem.text = dbLocation 							
		serversElem = et.SubElement(config,"dbServers")				#	sub element for server list
		if runSource == True:
			walkPath = source()											#	run source func to get walkPath list
		for index, name in enumerate(walkPath):
			index = "server" + str(index)
			et.SubElement(serversElem, index).set(index, name)
		et.ElementTree(config).write(xmlFileLocation)		#### write to xml file
		print "xml file created"

def setup():														#### Setup function to parse or setup config file
	global walkPath
	global dbLocation
	try:															#### routine to create xml config file ###
		config = et.parse(xmlFileLocation)
		print "found xml file"
		dbLocation = config.getiterator()[0]
		dbLocation = dbLocation.itertext()				### create dbLocation element from config element tree
		for i in dbLocation:										#### get first element in dbLocation element, should be actual location as string
			dbLocation = i
			break
		else:
			print "nothing to loop"
		serverList = config.getiterator()[2]
		for i in serverList:
			for i in i.items():
				print i[1]
				walkPath.append(i[1])
		
	except IOError, e:													#### create xml file if none is found ####
		createXML(True,True)

setup()															### setup config file using setup()

print "DB Location: ", dbLocation
conn = sqlite3.connect(dbLocation)								### setup query tool
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
															#######  SQL Commands ###########
															### create table with name "ingestIndex"
cursor.execute("""											
				CREATE TABLE IF NOT EXISTS ingestIndex(
					id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				filename TEXT,
				extension TEXT,
				size INTEGER,
				modDate INTEGER,
				checksum TEXT,
				directory INTEGER
				)
			""")

selectAll = """SELECT filename FROM ingestIndex"""
deleteRecordsByName = """DELETE FROM ingestIndex WHERE filename LIKE ?"""						
deleteAllRecords = """DELETE FROM ingestIndex WHERE id>-1"""			
createRecord = """INSERT INTO ingestIndex(filename, extension, modDate, directory, size) VALUES(?, ?, ?, ?, ?)"""
insertMD5 = """UPDATE ingestIndex set checksum=? WHERE id=?"""		
listTable = """SELECT * FROM ingestIndex"""		
searchTable = """ SELECT * FROM ingestIndex WHERE filename LIKE ?"""
		
														####### PYTHON Functions ###############
def sessionLog(logString):
	logFile = open("log.txt", 'w')
	logFile.write(logString)
	logFile.close()

def search(string, flag):										###### query/search DB with flags for options
	listOfSearchStrings = [] 
	searchString = string.split()
	for index, value in enumerate(searchString):				###	split strings into 5 substrings to search for
		listOfSearchStrings.append(value)
	while len(listOfSearchStrings) < 5:
		listOfSearchStrings.append("")
	string1 = listOfSearchStrings[0]
	string2 = listOfSearchStrings[1]
	string3 = listOfSearchStrings[2]	
	string4 = listOfSearchStrings[3]
	string5 = listOfSearchStrings[4]
	returnList = []
		
	try:		
		
		if flag == 'retID':							### flag to return only the rowID of input string
			cursor.execute(""" SELECT id FROM ingestIndex WHERE filename LIKE ?""", ('%'+string+'%',))			
			for row in cursor:
				return row[0]				

		if flag == 'findID':							### flag to search by rowID
			cursor.execute(""" SELECT filename FROM ingestIndex WHERE id LIKE ?""", ('%'+string+'%',))			
			for row in cursor:
				print row
				return
			
		if flag == 'string':						### flag to search filename by string
			cursor.execute(""" SELECT * FROM ingestIndex WHERE filename like ?""", ('%'+string1+'%'+string2+'%'+string3+'%'+string4+'%'+string5+'%',))
			for row in cursor:
				print row
			return
	
		if flag == 'outputFile':					### flag to search by filename and print results to text file
			textFile = getPath("Enter destination path for text file: ")
			cursor.execute(""" SELECT * FROM ingestIndex WHERE filename like ?""", ('%'+string1+'%'+string2+'%'+string3+'%'+string4+'%'+string5+'%',))
			for row in cursor:
				outPut('file', row, textFile) 
			return
		
		if flag == 'size':							### flag to select filesize and return
			cursor.execute(""" SELECT size FROM ingestIndex WHERE filename like ?""", (string,))
			for row in cursor:
				return row[0]
		
		if flag == 'enumerate':
			cursor.execute(""" SELECT * FROM ingestIndex WHERE filename like ?""", ('%'+string1+'%'+string2+'%'+string3+'%'+string4+'%'+string5+'%',))
			for row in cursor:
				returnList.append(row[0])
			return returnList			
				
				
	except UnboundLocalError,e:
		sessionLog(str("Error with argument: " + e))
		
				
		
def outPut(format, data, outFile):						####### Output to file or screen
	if format == 'screen':
		print data
		return
	if format == 'file':
		output = open(outFile, 'w')
		output.write(data)
		output.close()
		return


def copyMove(incoming, destination, CMFlag):				####### function to copy/move #############
	if CMFlag == 'c':											##### NOT COMPLETE
		if os.path.isdir(incoming):
			shutil.copytree(incoming, destination)					###NEED TO UPDATE DB WHEN copy is done
			updateIndex()
			return
		if os.path.isfile(incoming):
			shutil.copy2(incoming, destination)
			updateIndex()
			return
	elif CMFlag == 'm':
		shutil.move(incoming, destination)
		return
	else:
		print "Move/Copy not specified, action not completed"
		return

def addChecksum(incomingFile):
	filehash = hashlib.md5()							####### create checksum object
	key = os.path.basename(incomingFile)
	sourceFile = open(incomingFile,'rb')
	try:												
		while True:
			data = sourceFile.read(1048576)
			if len(data) == 0:
				break
			filehash.update(data)
			finalMD5 = filehash.hexdigest()
		rowID = search(incomingFile, 'retID')#[0]		### get rowID from search function...
		print "row ID: ", rowID							# ... index zero in return tuple
		cursor.execute(insertMD5, (finalMD5, rowID,))
		conn.commit()
		return 
			
	except IOError, e:
		sessionLog(str("\nIOError with file:  "+ fileentry + e))
	except OSError, e:
		sessionLog(str("\nOSError with file: " + e))	
		

def indexFiles():
	global walkPath 
	for server in walkPath:
		if os.path.exists(server):
			for root, dirs, files in os.walk(server):
				for file in files:
					try:		
						fileentry = unicode(os.path.join(root, file))		#create filepath
						if os.path.isdir(fileentry):
							isDir = 1
						else:
							isDir = 0			
						size = os.path.getsize(fileentry)
						modTime = unicode(os.path.getmtime(fileentry))		#get mod time
						extension = os.path.splitext(fileentry)[1]
						cursor.execute(createRecord, (fileentry, extension, modTime, isDir, size))	#enter into table
						conn.commit()
				
					except IOError, e:
						sessionLog(str("\nIOError with file:  "+ e + file))
					except OSError, e:
						sessionLog(str("\nOSError with file: "+ e + file))
					except UnicodeDecodeError:
						sessionLog(str("\nUnicode error: " + file))
		else:
			print "\nPath not available: ", server

			
def updateIndex():											####synchronize DB with paths

	while True:
		try:
			sessionLog("starting update")

			deleteList = []
			cursor.execute(selectAll)		# go through each row and look for file
			for row in cursor:
				if (os.path.isfile(row[0])) == False:				#if file does not exist remove from database
					deleteList.append(row[0])
					#print "added ",row[0], "to delete list"
			else:
				for i in deleteList:
					cursor.execute(deleteRecordsByName, (i,))
					conn.commit()
					
					sessionLog("deleted from database")

			for server in walkpath:	
				for root, dirs, files in os.walk(server):						## walk through target directory
					for file in files:											## find match in db
						file = os.path.join(root, file)					## if match doesn't exist:
						result = str(search(file, 'retID'))					## add match to database entry list
						#print result
						if result == 'None':
							print file, result
							modTime = str(os.path.getmtime(file))		#get mod time
							extension = os.path.splitext(file)[1]
							cursor.execute(createRecord, (str(file), extension, modTime))
							conn.commit() 
			sessionLog("Update complete")
	
		
		except IOError, e:
			sessionLog("IOError with file")
			sessionLog(str(e))
			sessionLog(str(row[0]))

														########### main ###################
def main():
	while True:
		command = str(raw_input("Enter command: ")).rstrip()
		

		if command == 'info':					### print info on DB and sources
			print "Config file location: ", os.path.abspath(xmlFileLocation)
			print "DB File Location: ", dbLocation
			print "Sources in DB: "
			for i in walkPath:
				print i
			
		if command == 'search':					### search for string in DB
			command = str(raw_input("Print results to file? (y/n) ")).rstrip()
			if command == ('n' or 'N'):
				string = str(raw_input("enter string to search for: ")).rstrip()
				search(string, 'string')
			if command == ('y' or 'Y'):
				string = str(raw_input("enter string to search for: ")).rstrip()
				search(string, 'outputFile')
		
		if command == 'source':					###setup and modify source directories
			source()							
			createXML(False, False)							### update the xml config file
			
		if command == 'index':					#### erase entire database and create new entries
			cursor.execute(deleteAllRecords)
			indexFiles()		
			
		if command == 'list':					#### print contents of database out to terminal
			print "listing..."
			cursor.execute(listTable)
			for row in cursor:
				print row

		if command == 'update':					###### launch update DB function in background process #####
			#updateIndex()
			p = m.Process(target=updateIndex,)
			p.start()									##### TODO implement background process
		
			
		if command == 'checksum':				##### create checksum(s) of file(s) and enter into database
			returnList = search('Search for file or path to create MD5: ', 'enumerate')
			enumList
			for index, file in enumerate(returnList):
				print index,file
				enumList.append(index)
				enumList.append(file)
			
			fileToRemove = raw_input("Enter index of file to remove: ")
			fileToRemove += 1
			fileToRemvoe = enumList([fileToRemove])
			
			if os.path.isdir(fileToRemove):
				print "directory"
				for root, dirs, files in os.walk(path):
					for file in files:									### run checksum in background process
						file = os.path.join(root, file)
						p = m.Process(target=addChecksum, args=(file,))						
						p.start()
	
			if os.path.isfile(fileToRemove):
				p = m.Process(target=addChecksum, args=(path,))
				p.start()	
		
		if command == "exit":
			exit()

													############### MAIN EXECUTION OF PROGRAM ###############

main()															### run main prompt


##################################### storage for functions that work while they are being revised #######################################



		if command == 'checksum':				##### create checksum(s) of file(s) and enter into database
			path = search('Search for file or path to create MD5: ', 'enumerate')
			if os.path.isdir(path):
				print "directory"
				for root, dirs, files in os.walk(path):
					for file in files:									### run checksum in background process
						file = os.path.join(root, file)
						p = m.Process(target=addChecksum, args=(file,))						
						p.start()
	
			if os.path.isfile(path):
				p = m.Process(target=addChecksum, args=(path,))
				p.start()	

import pypyodbc
import csv
import requests 
import ast
import json

URL = "https://pocrelativity/Relativity.REST/Workspace/1210338/Document/QueryResult"

project_name = "UK999"		# project code, e.g. UK999
initial_db = "1210338"		# project EDDS case ID
server_str = "UKVDS02395"	# server (not FQDN)
separator = "\\"			# forward/back slash
field = "PythonFilePath" 	# field containing the path to be stripped

params = {
	"condition": "'ArtifactId' IN SAVEDSEARCH 1052355",	#replace saved searchID
	"sorts": ["Artifact ID"],
	"fields": ["Artifact ID", "Doc ID Beg", "Original Folder Path"]
}

# here we should ask the user for creds
# as this is purely internal, no need to obfuscate these, as they will not be stored
kwarg = ('USERNAME', 'PASSWORD')

# verify set to false as there's a self signed cert on the POC
r = requests.post(URL, auth=kwarg, json=params, headers={'Content-type': 'application/json', 'Accept': 'json', 'x-csrf-header': ''}, verify=False)

# successful resource create
if r.status_code == 201:
	
	queryResult = json.loads(r.text)
	
	# queryResult is now type dict
	# Results is a list of dicts 
	filename = 'ss_output.txt'
	f = open(filename, 'w')

	# also create a list of tuples that will store the output
	provisional_res = []

	for it in queryResult["Results"]:
		# encode in UTF-8 to remove UNICODE 'u' from the strings
		stripped_path = (it["Original Folder Path"][it["Original Folder Path"].rfind(project_name):])[(it["Original Folder Path"][(it["Original Folder Path"].rfind(project_name)):]).find(separator):].encode("utf-8")
		identifier = it["Doc ID Beg"].encode("utf-8")
		pair = (identifier, stripped_path)
		provisional_res.append(pair)

		f.write(identifier + '\t' + stripped_path)
		f.write('\n')

	f.close()

	# now that we have the list, ask for input
	it = 1

	while True:
		print 'Please QC the output in the file: ' + filename
		qc = raw_input('Are you happy with the results? (y/n) ')

		# lowercase the input, get rid of whitespace
		qc = qc.lower()
		qc = qc.strip()

		if (qc == 'y'):
			break
		else:
			# here we ask for a specifier for further stripping
			nlvl = raw_input('Please specify the string to be used for further path removal: ')
			
			filename = 'iterated_' + it + '.txt'
			f = open(filename, 'w')

			# strip the path from the provisional result list
			for entry in provisional_res:
				entry[1] = (entry[1][entry[1].rfind(nlvl):])[(entry[1][(entry[1].rfind(nlvl)):]).find(separator):]

				f.write(entry[0] + '\t' + entry[1])
				f.write('\n')
			f.close()
			it += it

	print 'Commiting to the database EDDS{db}'.format(db=initial_db)

	# prepare the list of tuples for insertion into temp table
	values = ', '.join(map(str, provisional_res))

	connection_string = """Driver={{SQL Server Native Client 11.0}}; Server={server}.clientaccess.local; Database=EDDS{db}; Trusted_Connection=yes;""".format(server=server_str, db=initial_db)
	connection = pypyodbc.connect(connection_string)
	
	# create a list of commands
	cmd_list = []

	# due to API limitations this has to be executed in multiple steps, hence the 5 commands
	sqlcmd01 =	""" IF OBJECT_ID('EDDS{db}.EDDSDBO.PY_ProdPath') IS NOT NULL
						BEGIN DROP TABLE EDDS{db}.EDDSDBO.PY_ProdPath END
				""".format(db=initial_db)
	cmd_list.append(sqlcmd01)

	sqlcmd02 = 	"""	CREATE TABLE EDDSDBO.PY_ProdPath (
					DocIDBeg	NVARCHAR(50)
					,FilePath	NVARCHAR(MAX) )
				"""
	cmd_list.append(sqlcmd02)

	sqlcmd03 =	"""	INSERT INTO EDDSDBO.PY_ProdPath VALUES {val}
				""".format(val=values)
	cmd_list.append(sqlcmd03)

	sqlcmd04 = 	"""	CREATE CLUSTERED INDEX cix_docID ON EDDSDBO.PY_ProdPath (DocIDBeg)
				"""
	cmd_list.append(sqlcmd04)

	sqlcmd05 = 	""" UPDATE 		D
					SET 		D.{field} = PP.FilePath
					FROM 		EDDSDBO.Document D
					INNER JOIN 	EDDSDBO.PY_ProdPath PP
					ON 			D.DocIDBeg = PP.DocIDBeg
				""".format(field=field)
	cmd_list.append(sqlcmd05)

	# loop over commands and execute them
	for cmd in cmd_list:
		with connection.cursor() as cur:
			cur.execute(cmd)
			cur.commit()

	connection.close()

else:
	if (r.status_code == 400):
		print 'Credentials could not be verified.'
	elif (r.status_code == 401):
		print 'You are not authorized to access this workspace.'
	else:
		print 'Could not connect to the Workspace using REST API. Connection status code: ' + r.status_code
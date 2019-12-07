import sys
import os
import re
import csv
import sqlparse

table_schema={}
queryColumn=[]
tables=[]
natjoin=[]


#get column list of the table and list of list if table columns and stored table_namw:[listg of list of table columns]

def readMetaData():
	fp=open("metadata.txt","r")
	for line in fp:
		line=line.strip()
		if line =='<begin_table>':
			columns=[]
			flag=1
		elif flag==1:
			tablename=line
			flag=0
		elif line == '<end_table>':
			table_schema[tablename]=columns
		else:
			columns.append(line)

	print("Insise read")
	print "table_schema"
	print table_schema
	# print "columns"
	# print columns 



def findColIndex(col):
	ret=-1
	n=len(queryColumn)
	# print col
	print "Inside find column Index"
	print n,queryColumn


	for i in range(n):
		var=queryColumn[i]
		if col.lower() == var.lower():
			ret=i
		elif var.lower().endswith("."+col):
			ret=i
	return ret

def queryColumns(tablenames):
	# print tablenames
	query_cols = []
	print "Inside Query Columns"
	print tablenames
	for name in tablenames:
		if name in table_schema:
			schema = table_schema[name]
			for colname in schema:
				#submission format table1.A,table1.B
				query_cols.append(name+"."+colname)
		else:
			break
	print "query_cols"
	print query_cols

	return query_cols

def validColnameAndAggregate(cols,column,aggregate):
	#no column can exist with aggregate function
	print "Inside validColnameAndAggregate"
	cols = cols.split(",")
	errDist = 0
	errCol=0
	print "Cols"
	print cols
	n=len(cols)
	for i in range(n):
	    c = cols[i].strip()
	    #necessary everywhere to remove whitespace
	    if c.lower().startswith("max") or c.lower().startswith("min") or c.lower().startswith("sum") or c.lower().startswith("avg") :
	        if errCol==1:
	            return -1
	        else:
	            aggregate.append(c.split("(")[0])
	            column.append(c[4:len(c)-1])
	            errDist = 1
	    else:
	        if errDist == 1:#cae whre query like select max(A),B from table1 is wrong
	            return -1
	        else:
	            column.append(c)
	            errCol=1

	print "Column"
	print column


	return 0


def readTable(name,distflag):
	name=name+".csv"
	#each table is in csv file
	table=[]
	#valid file ame check i.e valid tablename check
	try:
		reader=csv.reader(open(name),delimiter=',')
        #csv reader
	except Exception, e:
	    #if table name not correct
	    print "Query not formed properly"
	    sys.exit()
	#handling if csv file contain some value in single or double quotes
	for row in reader:
		# print type(row)
		for i in range(len(row)):  
			if row[i][0] == "\'" or row[i][0] == '\"' :
				row[i] = row[i][1:-1]

		row = map(int,row)      #since each line of csv file is list of str and we here are dealing with only integers       
		if distflag == 1:
			if row not in table:
				table.append(row)
		else:
			table.append(row)

	print "Inside Read Table"
	print table

	return table



def getColumnNo(columns):
	print " Inside getColumn No"
	print columns

	resCol=[]
	if len(queryColumn)==0:
		return resCol
	if columns[0]=='*' and len(columns)==1:
		columns=queryColumn


	print columns
	k=len(natjoin)
	print k
	for col in columns:
		try:
			#this is for table.A
			checkdup=0
			colid=queryColumn.index(col)
			print "colid" 
			print colid
			if(k>0):
					# print "run"
					for i in range(k):
						# print("temp")
						if (natjoin[i][0] in resCol and natjoin[i][1]==colid) or (natjoin[i][1] in resCol and natjoin[i][0]==colid) or (natjoin[i][1] ==colid and natjoin[i][0]==colid):
							checkdup=1
					if checkdup==0:
						resCol.append(colid)
			else:
				resCol.append(colid)

		except ValueError:
            # print "E"
			flag = 0
			colid=0
			n=len(queryColumn)
			for i in range(n):
				var=quervalidColnameAndAggregateyColumn[i]
				if var.endswith("."+col):
					#this flag keeping track of ambigious columns
				    flag += 1
				    colid=i
			#this for single
			checkdup=0
			if(flag == 1):
				# print "k ",k
				if(k>0):
					# print "run"
					for i in range(k):
						# print("temp")
						if (natjoin[i][0] in resCol and natjoin[i][1]==colid) or (natjoin[i][1] in resCol and natjoin[i][0]==colid) or (natjoin[i][1] ==colid and natjoin[i][0]==colid):
							checkdup=1
					if checkdup==0:
						resCol.append(colid)


				else:
					resCol.append(colid)
			else:
			    return []
	print "natjoin"
	print natjoin

	print "resCol"
	print resCol
	
  	return resCol


def normalHeading(rescol):
    heading = []
    for i in rescol:
        heading.append(queryColumn[i])
    heading = ",".join(heading)
    heading += '\n'
    print "Normal Heading"
    print heading
    return heading


def getaggrHeading(column,aggregate):
    heading=""
    co = getColumnNo(column)
    if(len(co) == 0):
        print "Syntax error in columns"
        sys.exit()

    for i in range(len(co)):
        heading+=aggregate[i]+"("+queryColumn[co[i]]+"),"
   	#last , removal
    heading = heading[:-1]
    heading = heading+'\n'

    print "getaggrHeading"
    print heading

    return heading        

def join(tablenames,distflag):
	
	# print type(tablenames)
	n=len(tablenames)
	if n==1:
		return readTable(tablenames[0].strip(),distflag)
	else:
		tablenames = map(str.strip,tablenames)
		print tablenames
		k=0
		table=[]
		# print tablenames
		for name in tablenames:
			if k==0:
				# print "first ",name
				table = readTable(name,distflag)
				print "first",len(table)
			else:
				print "second ",name
				t=readTable(name,distflag)
				# print len(t)
				tlen=len(t)
				# print tlen
				
				temp=[]
				tablelen=len(table)
				print tablelen
				for i in range(0,tablelen):
					for j in range(0,tlen):
						temp.append(table[i]+t[j])
				table=temp
			k=k+1
			# print "last ",len(table)
	# print len(table)
	return table

def conSplit(condition):
    try:
        delimiters="and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        #and|or(?i)
        #? is lazy operator matches shortest possible string matches and|or
        con = re.split(regexPattern, condition)
        con = map(str.strip,con)
    except Exception as e:
        print "Syntax of and/or condition not correct"
        sys.exit()
    return con

def getOperands(con):
   
    operand = []
    try:
		#get the operator
		# relop = findRelationOperator(con)
		# print relop
		relop = ""
		i=0
		

		while i< len(con):
			if con[i] == '>' and con[i+1] == '=':
				relop = ">="
				i+=1
			elif con[i] == '>' and con[i+1] != '=':
				relop = ">"
				i+=1
			elif con[i] == '<' and con[i+1] == '=':
				relop = "<="
				i+=1
			elif con[i] == '<' and con[i+1] != '=':
				relop = "<"
				i+=1

			elif con[i] == '!' and con[i+1] == '=':
				relop = "!="
				i+=1
			elif con[i] == '=' and (con[i+1] != '=' or con[i+1] != '<'
								or con[i+1] != '>' or con[i+1] != '!'):

				relop = "="
				i+=1
			i+=1

		operand = con.split(relop)
		operand = map(str.strip,operand)
		#remove space around it(particularly handling space)
		if relop != "=":
			return operand,relop
		else:
			return operand,"=="

    except:
        print "Syntax Error in operand"
        sys.exit()

    return operand,relop

#this is for condition of natural jion after where i.e table1.B=table2.B
def columnBasedWhere(condition):
    global natjoin
    try:
        con=conSplit(condition)
        print "con " 
        print con
        for var in con:
            operand,operator = getOperands(var)
            print "operand"
            print operand 
            print "Operator"
            print operator
            operand = map(str.strip,operand)

            if '.' in operand[0] and '.' in operand[1]:
                if operator.strip() == "==":
                    sameCol = findColIndex(operand[0].strip()),findColIndex(operand[1].strip())
                    print "sameCol"
                    print sameCol
                    natjoin.append(sameCol)

    except Exception as e:
        print "Syntax Error in operand,operator around each (and/or) operator"
        sys.exit()

def evaluateWhereValueBased(condition):
	try:
		connector=[]
		cond=condition.split(" ")
		print "Inside evaluateWhereValueBased"
		print cond

		cond=map(str.strip,cond)
		n=len(cond)

		for i in range(n):
			temp=cond[i].lower().strip()
			if temp=="and":
				connector.append(temp)
			if temp=="or":
				connector.append(temp)

		#first get the condition on left and right side of connector
		con=conSplit(condition)
		# print con
		i=0
		for var in con:
			#now get operands around each sub condition and identify the operator
			operand,operator=getOperands(var)
			# print operand,operator
			#a b ==/a b <
			lhs = findColIndex(operand[0].strip())
			rhs = findColIndex(operand[1].strip())
			# print "yes",lhs,rhs
			if lhs >-1 and rhs >-1:
				operand[0] = operand[0].replace(operand[0],"tables[i]["+str(lhs)+"]")
				operand[1] = operand[1].replace(operand[1],"tables[i]["+str(rhs)+"]")

			elif lhs>-1:
				operand[0] = operand[0].replace(operand[0],"tables[i]["+str(lhs)+"]")
				# print operand[0]
			else:
				print "Syntax error in getting condition in findcol in evalwhervalues"
				sys.exit()

			t = operand[0],operand[1]
			# print t
			# ('result[i][0]', '500')
			con[i] = operator.join(t)
			# print i,con[i]
			i=i+1

		x=0
		j=0
		newcon=""
		# print "ok",con[0],con[1]
		# print connector[0]
		for var in con:
			# print "ss",var
			if j==0:
				
				newcon=var+" "
				# print "sssss",newcon
			else:
				# print connector[0]
				conj=connector[0]
				newcon=newcon+conj+" "
				# print "af",newcon
				newcon=newcon+var+" "
				# print "2",newcon
			j=j+1
			x=x+1
		res = []
		# print newcon
		# print len(tables)
		print "newcon"
		print newcon
		for i in range(len(tables)):
			if eval(newcon):
				print tables[i]
				res.append(tables[i])

	except Exception as e:
		print "Syntax Error in getting condition in eval format"
		sys.exit()

	return res

def calAggr(col):
	ind = getColumnNo(col)
	if len(ind)==0:
		print "Syntax error in column:might be ambigious"
		sys.exit()
	temp = []
	n=len(tables)
	for i in range(n):
		temp.append(tables[i][ind[0]])

	return temp

def aggFunction	(column,aggregate):
	ans=""
	d=0
	for col in aggregate:
		if col.lower()=="min":
			try:
				m = min(calAggr(column[d]))
				# print "abc",m
			except ValueError:
			    m = 'null'
			ans=ans+str(m)+"\t"

		if col.lower()=="max":
			try:
			    m = max(calAggr(column[d]))
			except ValueError:
			    m = 'null'
			ans=ans+str(m)+"\t"

		if col.lower()=="sum":
			try:
			    m = sum(calAggr(column[d]))
			except ValueError:
			    m = 'null'
			ans=ans+str(m)+"\t"

		if col.lower()=="avg":
			try:
				m = sum(calAggr(column[d]))
				m= float(float(m)/len(result))
				m = float("{0:.2f}".format(m))
			except ValueError:
			    m = 'null'
			ans=ans+str(m)+"\t"
		d=d+1
	return ans



def queryEvaluation(query):
	parsedQuery=sqlparse.parse(query)[0].tokens
	#sql.parse return tuple
	#[<DML 'Select' at 0x7F8696CF9A78>, <Whitespace ' ' at 0x7F8696CC1050>, <Wildcard '*' at 0x7F8696CC10B8>, <Whitespace ' ' at 0x7F8696CC1120>, <Keyword 'from' at 0x7F8696CC1188>, <Whitespace ' ' at 0x7F8696CC11F0>, <Identifier 'table2' at 0x7F8696CB28D0>]
	#print parsedQuery
	#first parse the query using sql parse and get the generator to traverse parsedQuery
	c=sqlparse.sql.IdentifierList(parsedQuery).get_identifiers()
	#c is generator iterator(a generator is a function that returns an object (iterator) which we can iterate over (one value at a time).)
	queryParts=[]
	k=0
	for part in c:
		queryParts.append(str(part))
		# print str(part).lower()
		#handling select
		if k==0 and str(part).lower() != 'select':
			print "Query without select not allowed"
			sys.exit()
		k=k+1
	# print queryParts
	n=len(queryParts)
	if n<4:
		print "Syntax error in query: Query length less than 4 not possible"
		sys.exit()
	# print("djhbd")
	tablenames=""
	flag=0
	where=0
	condition=""
	for i in range(n):
		part=queryParts[i].lower()
		if part=="distinct":
			flag=flag+1
		elif part=="from":
			tablenames=queryParts[i+1]
		elif part.startswith('where'):
			condition=part[6:].strip()
			where=1
	
	# if where == 0 :
	# 	print "dds"		
	# 	print "Some unknown part in Query"
	# 	sys.exit()
	#get column and ensure its validness
	if(flag>1):
		print "Syntax error with the usage of distinct(possible more than one distinct)"
		sys.exit()

	if where ==1 and len(condition.strip())==0 :
		print("Syntax error in where clause")
		sys.exit()
		# print "Syntax error in where clause"
		# sys.exit()
    
	if len(queryParts)> 5 and where ==0 :
		print "Syntax error (Query length without where can't be greater than 5"
		sys.exit()

	if len(queryParts)== 5 and where ==0 and flag==0:
		print "Syntax error (Query length without where and without distinct can't be equal to 5"
		sys.exit()
	# print where
	cols=""
	if flag==0:
		cols=queryParts[1]
	else:
		cols=queryParts[2]
	# print cols
	global queryColumn,tables
	column=[]
	aggregate=[]
	err=validColnameAndAggregate(cols,column,aggregate)
	print "err"
	print err
	##checked validness
	if err==-1:
		print "Syntax error - aggregate columns used with normal columns"
		sys.exit()
	##now handling the tables##
	tablename=tablenames.split(",")
	# print tablename
	queryColumn = queryColumns(tablename)
	ans=""
	tables=join(tablename,flag)
	# print "awes",len(tables)
	# print "ddd",tables
	#we got the tables in form of list of list
	#order of eval
	#first condition evaluate(i.e where clause)
	#than column selected
	#than aggregate applied 
	# print queryColumn
	if condition !="":
		tables = evaluateWhereValueBased(condition)
		# print tables
		# If there are aggregate functions with where condition
		if len(aggregate)>0:
			heading=getaggrHeading(column,aggregate)
		#colum based where or like natural join
		# print "REPLICA"
		# print len(tables)
		columnBasedWhere(condition)
		#i.e get index of replica columns
		# print "nat ",natjoin
		
	if len(aggregate)==0:
		# print column
		resultcol=getColumnNo(column)
		#sare required column ke column number aa gaye
		# print resultcol
		if len(resultcol) == 0:
			print "Syntax error - might have ambigious column "
			sys.exit()
		heading=normalHeading(resultcol)
		tablen=len(tables)
		len_rescol=len(resultcol)
		for i in range(tablen):
			for j in range(len_rescol):
				ans+=str(tables[i][resultcol[j]])+"\t"

			ans+='\n'
	else:
		try:
			heading=getaggrHeading(column,aggregate)
			# print heading
			if len(heading)>0:
				# print column
				ans+=aggFunction(column,aggregate)
				# print ans
			else:
				ans = 'null'
		except IndexError as e:
			print "Syntax error in getting aggQueryAns"

	if flag == 1:
		try:
			row = ans.split('\n')
			nr = []
			for r in row:
				if r not in nr:
					nr.append(r)
			ret = '\n'.join(nr)
		except Exception:
			print "Syntax Error in getting distinct row in ans"
			sys.exit()
		ans = ret

	if ans == "":
		print "Empty"
	else:
		print heading+ans
    	


def main():
	readMetaData()

	#Read query

	query=sys.argv[1]
	validQuery=query.split(";")
	if len(validQuery)==1:
		print "Syntax Error Semicolon Required"
		sys.exit()
	elif len(validQuery)>2:
		print "Syntax Error in Query"
		# print validQuery[1]
		sys.exit()
	elif len(validQuery)==2 and len(validQuery[1])!=0:   # case like ;select * from abc this case
		print "Syntax Error in Query"
		# print validQuery[1]
		sys.exit()

	query=validQuery[0]
	query.strip()
	# print query
	queryEvaluation(query)

if __name__ == "__main__":
	main()


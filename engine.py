import sys
import re
import os
sys.path.insert(0,os.getcwd() + "/sqlparse-0.2.4")
import sqlparse
import csv

metadata = 'metadata.txt'
# hold the format of our table and its attribute
table_schema={}
#contain schema of particulat table as key
query_cols = []
#contain all table1.A,table1.B,table1.C,table2.B,table.D
nat_join = []

result = []
#contain table in form of 2D list

def getColnameAndAggregate(cols,column,aggr):
    cols = cols.split(",")
    errDist = 0
    for i in range(len(cols)):
        c = cols[i].strip()
        #necessary everywhere to remove whitespace
        if c.lower().startswith("max") or c.lower().startswith("min") or \
        c.lower().startswith("sum") or c.lower().startswith("avg") :
            aggr.append(c.split("(")[0])
            column.append(c[4:len(c)-1])
            errDist = 1
        else:
            if errDist == 1:#cae whre query like select max(A),B from table1 is wrong
                return -1
            else:
                column.append(c)
    return 0
def andSplit(condition):
    try:
        delimiters="and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        #and|or(?i)
        #? is lazy operator matches shortest possible string matches and|or
        con = re.split(regexPattern, condition)
        con = map(str.strip,con)
    except Exception as e:
        print "Syntax Error"
        sys.exit()
    return con

def readMetaData():
    f = open(metadata,"r")

    for line in f:
        if line.strip() == '<begin_table>':
            columns = []
            #cnt = 0
            flag=1
            continue
        if line.strip() == '<end_table>':
            table_schema[tablename] = columns
            continue
        if flag==1:
            tablename=line.strip()
            flag=0
            continue
        columns.append(line.strip())
        
#reading table and converting it into 2 D list
def readTable(name,distinct):
    name = name + ".csv"
    #all table in csv file
    result = []
    #list of list that contain tables
    try:
        reader=csv.reader(open(name),delimiter=',')
        #csv reader
    except Exception, e:
        #if table name not correct
        print "Query not formed properly"
        sys.exit()
    for row in reader:
        for i in range(len(row)):
             #handling if csv file contain some value in single or double quotes
            if row[i][0] == "\'" or row[i][0] == '\"':
                row[i] = row[i][1:-1]
        # print(type(row)) 
        # print(type(row[0]))  

        row = map(int,row)      #since each line of csv file is list of str and we here are dealing with only integers
        # print(type(row[0]))   
        temp = []
        # for i in range(len(row)):
        #     temp.append(int(row[i]))
        temp.append(row)

        if distinct == 1:
            if row not in result:
                result.append(row)
        else:
            result.append(row)

    return result

# appending each row of one table with every other row of table2 ans so on
def join(table_names,distinct):
    if len(table_names) == 1:
        return readTable(table_names[0],distinct)
    else:
        table = readTable(table_names[0],distinct)
        #now table is 2d list
        for i in range(1,len(table_names)):
            #read each table one-by-one
            t = readTable(table_names[i],distinct)
            temp = []
            #append every row of second in first and this goes on replacing table with new append
            for j in range(0,len(table)):
                for k in range(0,len(t)):
                    temp.append(table[j]+t[k])
            table = temp
    return table

def naturalJoin(condition):
    global nat_join
    try:
        con=andSplit(condition)
        for i in range(len(con)):
            split = getOperands(con[i])
            split = map(str.strip,split)

            if '.' in split[0] and '.' in split[1]:
                if split[2].strip() == "==":
                    same = findColIndex(split[0].strip()),findColIndex(split[1].strip())
                    nat_join.append(same)

    except Exception as e:
        print "Syntax Error "
        sys.exit()
#making submission format
def queryColumns(table_names):
    query_cols = []
    # print table_names
    for i in range(len(table_names)):
        if table_names[i] in table_schema:
            schema = table_schema[table_names[i]]

            for y in range(len(schema)):
                #submission format table1.A,table1.B
                query_cols.append(table_names[i]+"."+schema[y])

        else:
            return []

    return query_cols

def findColIndex(col):
    ret = -1
    y = 0
    #query_cols in format table1.A
    for cols in query_cols:
        if cols.endswith("."+col): #for A in table1
            ret = y
        elif cols.lower() == col.lower():#for table1.A in table1
            ret=y
        y+=1
    return ret

#checking valid relation operator
def findRelationOperator(con):
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
    return relop


def getOperands(con):
   
    operand = []
    try:
        #get the operator
        relop = findRelationOperator(con)
        # print relop
        operand = con.split(relop)
        operand = map(str.strip,operand)
        #remove space around it(particularly handling space)
        if relop != "=":
            operand.append(relop)
        else:
            operand.append("==")

    except:
        print "Syntax Error in operand"
        sys.exit()

    return operand

#evaluate first and | or than the conditions around them
#getting in format of eval
def evaluateWhere(condition):
    # global error

    try:
        arr = condition.split(" ")
        arr = map(str.strip,arr)
        connector = []
        for ar in arr:
            if ar.lower().strip() == "and" or ar.lower().strip() == "or":
                # print(ar.lower())
                connector.append(ar.lower())
        con=andSplit(condition)

        for i in range(len(con)) :

            operand = getOperands(con[i])
            operand = map(str.strip,operand)
            #a b ==/a b <
            lhs = findColIndex(operand[0].strip())
            rhs = findColIndex(operand[1].strip())

            if lhs >-1 and rhs >-1:
                operand[0] = operand[0].replace(operand[0],"result[i]["+str(lhs)+"]")
                operand[1] = operand[1].replace(operand[1],"result[i]["+str(rhs)+"]")

            elif lhs>-1:
                operand[0] = operand[0].replace(operand[0],"result[i]["+str(lhs)+"]")
                
            else:
                print "Syntax error"
                sys.exit()

            t = operand[0],operand[1]
            # print t
            # ('result[i][0]', '500')
            con[i] = operand[2].join(t)
            #x.A > 500 translate to result[i][colindex] > 500

        new_con = con[0]+" "
        #print new_con
        x = 0
        for j in range(1,len(con)):
            new_con+= connector[x].lower()+" "
            new_con+=con[j]+" "
        #wapas end se append
        # print new_con
        #result[i][0]>500 and result[i][0]<900
        res = []
        for i in range(len(result)):
            if eval(new_con):
                res.append(result[i])

    except Exception as e:
        print "Syntax Error"
        sys.exit()

    return res





def aggFunctions(column,aggr):
    ans = ""
    for i in range(len(column)):
        if aggr[i].lower() == "max":
            ind = select_columns([column[i]])
            temp = []

            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = max(temp)
            except ValueError:
                m = 'null'

            ans+=str(m)+"\t"

        elif aggr[i].lower() == "min":
            ind = select_columns([column[i]])
            temp = []

            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = min(temp)
            except ValueError:
                m = 'null'

            ans+=str(m)+"\t"

        elif aggr[i].lower() == "sum":
            ind = select_columns([column[i]])
            temp = []
            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = sum(temp)
            except ValueError:
                m = 'null'
            ans+=str(m)+"\t"

        elif aggr[i].lower() == "avg":
            ind = select_columns([column[i]])
            temp = []
            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = sum(temp)
                m= float(float(m)/len(result))
                m = float("{0:.2f}".format(m))
            except Exception:
                m  = 'null'
            ans+=str(m)+"\t"
    return ans



def select_columns(column):

    if len(query_cols) == 0:
        return []

    res_col = []
 #just convert star into list and updated column in this case as whole
    if ''.join(column) == '*':
        # print column
        # print ''.join(column)
        column = query_cols
    for col in column:
        try:
            res_col.append(query_cols.index(col))
        ########################no need of except#######################(just iterating same thing)
        except ValueError:
            flag = 0
            search = ""
            for space in query_cols:
                if space.endswith("."+col):
                    flag += 1
                    search = space

            if(flag == 1):
                index = query_cols.index(search)
                res_col.append(index)
            else:
                return []

    if(len(nat_join)>0):
        for i in range(len(nat_join)):
            if nat_join[i][0] in res_col and nat_join[i][1] in res_col:
                i1 = res_col.index(nat_join[i][0])
                i2 = res_col.index(nat_join[i][1])
                if i1<i2:
                    del res_col[i2]
                else:
                    del res_col[i1]

    if(len(res_col) == 0):
        print "Syntax error in columns"
        sys.exit()

    return res_col

def different(ans):
    try:
        row = ans.split('\n')
        nr = []
        for r in row:
            if r not in nr:
                nr.append(r)
        ret = '\n'.join(nr)
    except Exception:
        print "Syntax Error"
        sys.exit()
    return ret


def process_query(query):
    global query_cols,result

    parsed_query = sqlparse.parse(query)[0].tokens
    command = sqlparse.sql.Statement(parsed_query).get_type()

    if command.lower() != 'select':
        print "Invalid query"
        sys.exit()

    components = []
    c = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
    for i in c:
        components.append(str(i))
    #command,attribute,from,tablemame,where+codition,distinct,w/o distinct minimum 4 component
    flag = 0
    where = 0
    table_names = ""
    condition = ""
    cols = ""
    if len(components)<4:
        print "Syntax error" 
        sys.exit()
    for i in range(len(components)):
        if components[i].lower() == "distinct":
            flag+=1
        elif components[i].lower() == "from":
            table_names = components[i+1]
        elif components[i].lower().startswith('where'):
            where=1
            condition = components[i][6:].strip()

    if(flag>1):
        print "Syntax error with the usage of distinct(possible more than one distinct)"
        sys.exit()

    if where ==1 and len(condition.strip())==0:
        print "Syntax error in where clause"
        sys.exit()
    
    if len(components)> 5 and where ==0 :
        print "Syntax error"
        sys.exit()

    if len(components)== 5 and where ==0 and flag==0:
        print "Syntax error"
        sys.exit()

    if flag == 1:
        cols = components[2]
        #i.e component[1] is Distinct
        #cols is which column to take
    else:
        cols = components[1]

    # print cols
    # print table_names
    # print condition
    column = []
    aggr = []
    error = getColnameAndAggregate(cols,column,aggr)
    if error == -1:
        print "Syntax error - aggregate columns used with normal columns"
        sys.exit()

    table_names = table_names.split(",")
    table_names = map(str.strip,table_names)
    query_cols = queryColumns(table_names)
    #print format mei hi hai
    result = []
    heading = ""

    result = join(table_names,flag)
    #flag for distinct
    # If there is a where condition
    if condition != "":
        result = evaluateWhere(condition)
        # If there are aggregate functions with where condition
        ###############################LEFT###########################################
        if len(aggr)>0:
            co = select_columns(column)
            for i in range(len(co)):
                heading+=aggr[i]+"("+query_cols[co[i]]+"),"
            heading = heading[:-1]
            heading = heading+'\n'
        naturalJoin(condition)


    ans = ""

    #If aggregate functions are used
    #JUST two string heading and result as final ans
    if len(aggr) == 0:
        res_col = select_columns(column)
        if len(res_col) == 0:
            print "Syntax error - result is null "
            sys.exit()
        heading = []
        for i in res_col:
            heading.append(query_cols[i])
        heading = ",".join(heading)
        heading += '\n'

        for i in range(len(result)):
            for j in range(len(res_col)):
                ans+=str(result[i][res_col[j]])+"\t"

            ans+='\n'

    # If aggregate functions are not used
    else:
        try:
            heading = ""
            co = select_columns(column)
            for i in range(len(co)):
                heading+=aggr[i]+"("+query_cols[co[i]]+"),"
            heading = heading[:-1]
            heading = heading+'\n'

            if len(heading)>0:
                ans+=aggFunctions(column,aggr)
            else:
                ans = 'null'
        except IndexError as e:
            print "Syntax error"

    if flag == 1:
        ans = different(ans)

    if ans == "":
        print "Empty"
    else:
        print heading+ans



def main():
    readMetaData()
    query = sys.argv[1]
    query = query.split(";")[0]
    query.strip()
    process_query(query)

if __name__ == "__main__":
    main()

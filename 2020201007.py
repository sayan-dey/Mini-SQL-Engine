'''
Mini SQL Engine
by Sayan Dey

'''

import  os, sys, re, csv
from ordered_set import OrderedSet
import sqlparse
from itertools import product

# Assuming col_names will be a single char

#Reading metadata
def read_data():
    meta= open('metadata.txt', 'r')
    Lines= meta.readlines()
    flag=0
    for line in Lines:
        if line.strip() == '<begin_table>':
            flag=1
            col_list=[]
        elif flag==1:
            tabname=line.strip()
            tabname=tabname.upper()
            flag=0
        elif line.strip() == '<end_table>':
            tab_schema[tabname]=col_list
            flag=0    
        elif flag==0:
            temp=line.strip()
            col_list.append(temp.upper())


def cartesian_prod(q_lis):
    if q_lis[1].upper()=='DISTINCT':
        temp=q_lis[4]
    else:
        temp=q_lis[3]
    table_list = re.split(', |,',temp)   #list of tables in the query 
    full_tuple_list = []  #its each element contains tuples' list of 1 table
    
    for i in range(0,len(table_list)):
        tab1=open(table_list[i]+'.csv','r')
        Lines=tab1.readlines()
        tuple_list = []  #contains tuples of 1 table
        for line in Lines:
            temp=re.split(',',line.strip())
            tuple_list.append(temp)
        full_tuple_list.append(tuple_list)            
    
    if len(table_list) > 1:
        res_list=list(product(full_tuple_list[0],full_tuple_list[1])) 
        res_list1=[]

        for x in res_list:
            temp=[]
            for y in x:
                for z in y:
                    temp.append(z)
            res_list1.append(temp)     

        for i in range(2,len(table_list)):
            res_list=list(product(res_list1,full_tuple_list[2]))
            res_list1=[]
            for x in res_list:
                temp=[]
                for y in x:
                    for z in y:
                        temp.append(z)
                res_list1.append(temp)

    else:
        res_list1=full_tuple_list[0]             
    # res_list1 is final result of join     
    return res_list1         



def process_query(q_lis):
    res_head=''  #string to store heading of o/p

    if leng==4:
        
        if q_lis[1][-1:] == ')':  # aggregate function (cart_prod supported)
            table_list = re.split(', |,',q_lis[3])   #list of tables in the query
            aggr=''
            for x in q_lis[1]:
                if x!='(':
                    aggr+=x
                else:
                    break 
            col=q_lis[1][q_lis[1].index(x)+1:-1]  #col_name on which aggr fn will work 

            col_list=[]  # list of all cols of tables in this query
            for x in table_list:
                temp=tab_schema[x.upper()]
                for y in temp:
                    col_list.append(y)

            res_head=q_lis[1]
            
            #tab1=open('files/'+q_lis[3]+'.csv','r')
            #Lines=tab1.readlines()
            Lines=cartesian_prod(q_lis)
            if col!='*':
                pos=col_list.index(col.upper())
                col_list=[]
                for line in Lines:
                    temp=line
                    col_list.append(int(temp[pos])) 
            if aggr.upper()=='MAX':
                print(res_head.lower())     
                print(max(col_list))
            elif aggr.upper()=='MIN':
                print(res_head.lower())
                print(min(col_list))        
            elif aggr.upper()=='SUM':
                print(res_head.lower())
                print(sum(col_list))
            elif aggr.upper()=='COUNT':
                print(res_head.lower())
                if(col=='*'):
                    print(len(Lines))
                else:
                    print(len(col_list))
            elif aggr.upper()=='AVG':
                print(res_head.lower())
                print (sum(col_list)/len(col_list))
            else:
                sys.exit('Aggregate function does not exist')                

        else:  # print 1 or more cols of a table  (cart_prod supported)
            table_list = re.split(', |,',q_lis[3])   #list of tables in the query
            col_list=[]  # list of all cols of tables in this query
            for x in table_list:
                temp=tab_schema[x.upper()]
                for y in temp:
                    col_list.append(y)
            if q_lis[1]!='*':
                dis_list = re.split(', |,',q_lis[1])  #list of cols to be displayed
            else:
                dis_list = col_list    
            for x in dis_list:
                res_head+=x+','
            res_head=res_head[:-1]    
            pos_list=[]   #list to store req col no.s to be displayed
            for x in dis_list:
                for y in col_list:
                    if x.upper()==y:
                        pos_list.append(col_list.index(y))
                        break
            if(len(pos_list)!=len(dis_list)):
                sys.exit('Mismatch between column name and table name')
            print(res_head.lower())    
            #tab1=open('files/'+q_lis[3]+'.csv','r')  
            #Lines= tab1.readlines()
            Lines=cartesian_prod(q_lis)
            for line in Lines:
                temp=line
                st=''
                for x in pos_list:
                    st+=str(temp[x])+','
                st=st[:-1]    
                print(st)                   


    elif leng==5:
        if q_lis[1].upper()=='DISTINCT':   # select distinct rows (cart_prod supported)
            table_list = re.split(', |,',q_lis[4])   #list of tables in the query
            col_list=[]  # list of all cols of tables in this query
            for x in table_list:
                temp=tab_schema[x.upper()]
                for y in temp:
                    col_list.append(y)
            dis_list = re.split(', |,',q_lis[2])  #list of cols to be displayed
            for x in dis_list:
                res_head+=x+','
            res_head=res_head[:-1]    
            pos_list=[]   #list to store req col no.s to be displayed
            for x in dis_list:
                for y in col_list:
                    if x.upper()==y:
                        pos_list.append(col_list.index(y))
                        break
            if(len(pos_list)!=len(dis_list)):
                sys.exit('Mismatch between column name and table name')
            print(res_head.lower())    
            #tab1=open(q_lis[4]+'.csv','r')  
            #Lines= tab1.readlines()
            Lines=cartesian_prod(q_lis)
            res_set =OrderedSet()  #using set for 'distinct'
            for line in Lines:
                temp=line
                st=''
                for x in pos_list:
                    st+=str(temp[x])+','
                st=st[:-1]    
                res_set.add(st)
            for x in res_set:
                print(x)          

        else:  #select col1,col2,...coln from tablename where col1>num ... (cart_prod supported)
            table_list = re.split(', |,',q_lis[3])   #list of tables in the query
            col_list=[]  # list of all cols of tables in this query
            for x in table_list:
                temp=tab_schema[x.upper()]
                for y in temp:
                    col_list.append(y)   

            pos=0
            # logic for supporting aggr func with 'WHERE'
            if q_lis[1][-1:] == ')':  # aggregate function (cart_prod supported)
                aggr=''
                for x in q_lis[1]:
                    if x!='(':
                        aggr+=x
                    else:
                        break 
                col=q_lis[1][q_lis[1].index(x)+1:-1]  #col_name on which aggr fn will work 

                res_head=q_lis[1]
            
                Lines=cartesian_prod(q_lis)
                if col!='*':
                    pos=col_list.index(col.upper())

            else:
                if q_lis[1]!='*':
                    dis_list = re.split(', |,',q_lis[1])  #list of cols to be displayed
                else:
                    dis_list = col_list

                for x in dis_list:
                    res_head+=x+','
                res_head=res_head[:-1]

                pos_list=[]   #list to store req col no.s to be displayed
                for x in dis_list:
                    for y in col_list:
                        if x.upper()==y:
                            pos_list.append(col_list.index(y))
                            break
                if(len(pos_list)!=len(dis_list)):
                    sys.exit('Mismatch between column name and table name')
                print(res_head.lower())       

            q_lis[4]=q_lis[4].upper()
            cond_list=re.split(' ',q_lis[4])
            flag=-1
            if len(cond_list)>2:
                if cond_list[2]=='AND':
                    flag=0
                elif cond_list[2]=='OR':
                    flag=1
            cond_list=re.split('WHERE | AND | OR ',q_lis[4]) #list to store conds like A>=5
            cond_list=cond_list[1:]
            for x in range(0,len(cond_list)):
                cond_list[x]=cond_list[x].replace(' ','')
            
            pos_cond_list=[]  #list to store pos of cols on which cond present
            for x in cond_list:
                pos_cond_list.append(col_list.index(x[0]))

            #tab1=open('files/'+q_lis[3]+'.csv','r')  
            #Lines= tab1.readlines()
            Lines=cartesian_prod(q_lis)
            res_list= []  # no need to make it a set
            for line in Lines:
                temp=line
                for x in range(0,len(cond_list)):                    
                    chk=0  # 0->include, else->exclude
                    if cond_list[x][1]=='<' and cond_list[x][2]=='=':
                        temp1=cond_list[x].split('<=')
                        if flag==0:
                            if int(temp[pos_cond_list[x]])>int(temp1[1]):
                                chk+=1
                                break
                        else:
                             chk=1
                             if int(temp[pos_cond_list[x]])<=int(temp1[1]):
                                chk=0
                                break     

                    elif cond_list[x][1]=='>' and cond_list[x][2]=='=':
                        temp1=cond_list[x].split('>=')
                        if flag==0:
                            if int(temp[pos_cond_list[x]])<int(temp1[1]):
                                chk+=1
                                break 
                        else:
                             chk=1
                             if int(temp[pos_cond_list[x]])>=int(temp1[1]):
                                chk=0
                                break 

                    elif cond_list[x][1]=='<':
                        temp1=cond_list[x].split('<')
                        if flag==0:
                            if int(temp[pos_cond_list[x]])>=int(temp1[1]):
                                chk+=1
                                break 
                        else:
                             chk=1
                             if int(temp[pos_cond_list[x]])<int(temp1[1]):
                                chk=0
                                break 

                    elif cond_list[x][1]=='>':
                        temp1=cond_list[x].split('>')
                        if flag==0:
                            if int(temp[pos_cond_list[x]])<=int(temp1[1]):
                                chk+=1     
                                break
                        else:
                             chk=1
                             if int(temp[pos_cond_list[x]])>int(temp1[1]):
                                chk=0
                                break 

                    elif cond_list[x][1]=='=':
                        temp1=cond_list[x].split('=')
                        if flag==0:
                            if int(temp[pos_cond_list[x]])!=int(temp1[1]):
                                chk+=1 
                                break
                        else:
                             chk=1
                             if int(temp[pos_cond_list[x]])==int(temp1[1]):
                                chk=0
                                break    
                if chk==0:
                    if q_lis[1][-1:] == ')':
                        res_list.append(int(temp[pos]))
                    else:        
                        st=''
                        for x in pos_list:
                            st+=str(temp[x])+','
                        st=st[:-1]    
                        res_list.append(st)
               
            
            if q_lis[1][-1:] == ')':
                if aggr.upper()=='MAX':
                    print(res_head.lower())     
                    print(max(res_list))
                elif aggr.upper()=='MIN':
                    print(res_head.lower())
                    print(min(res_list))        
                elif aggr.upper()=='SUM':
                    print(res_head.lower())
                    print(sum(res_list))
                elif aggr.upper()=='COUNT':
                    print(res_head.lower())
                    if(col=='*'):
                        print(len(res_list))
                    else:
                        print(len(res_list))
                elif aggr.upper()=='AVG':
                    print(res_head.lower())
                    print (sum(res_list)/len(res_list))
                else:
                    sys.exit('Aggregate function does not exist')

            else:
                for x in res_list:
                    print(x)    


    elif leng==6:
        if q_lis[4].upper()=='ORDER BY':  # select col1,col2,... from tablename order by col ASC/DESC; (cart_prod supported)
            table_list = re.split(', |,',q_lis[3])
            col_list=[]  # list of all cols of tables in this query
            for x in table_list:
                temp=tab_schema[x.upper()]
                for y in temp:
                    col_list.append(y)
            if q_lis[1]!='*':
                dis_list = re.split(', |,',q_lis[1])
            else:
                dis_list=col_list    
            for x in dis_list:
                res_head+=x+','
            res_head=res_head[:-1]

            pos_list=[]   #list to store req col no.s to be displayed
            for x in dis_list:
                for y in col_list:
                    if x.upper()==y:
                        pos_list.append(col_list.index(y))
                        break
            if(len(pos_list)!=len(dis_list)):
                sys.exit('Mismatch between column name and table name')
            print(res_head.lower()) 

            temp1=re.split(' ',q_lis[5]) 
            ordr = temp1[1].upper()  # ASC or DESC
            pos = col_list.index(temp1[0].upper()) # pos of col to be sorted 

            #tab1=open('files/'+q_lis[3]+'.csv','r')  
            #Lines= tab1.readlines()
            Lines=cartesian_prod(q_lis)
            res_list =[]
            tuple_list =[]  # to store tuples
            val_list =[]  # to store values of col to be sorted
            for line in Lines:
                temp=line
                for x in range(0,len(temp)):
                    temp[x]=int(temp[x])  # temp is storing all the values (int) of this tuple
                tuple_list.append(temp)
                val_list.append(temp[pos])
            
            if ordr == 'DESC':
                val_list.sort(reverse=True)
            else:
                val_list.sort(reverse=False)    
            for x in range(0,len(val_list)):
                for y in range(0,len(tuple_list)):
                    if val_list[x]==tuple_list[y][pos]:
                        res_list.append(tuple_list[y])
                        tuple_list.remove(tuple_list[y])
                        break  
            
            for x in res_list:
                st=''
                for y in pos_list:
                    st+=str(x[y])+','
                st=st[:-1]
                print(st)    

        elif q_lis[4].upper()=='GROUP BY':  #select col1, aggr(col2) from tablename group by col1;
            col_list=tab_schema[q_lis[3].upper()]
            dis_list = re.split(', |,',q_lis[1])
            if dis_list[1][-1:]==')':
                temp=dis_list[1]
                pos=col_list.index(dis_list[0].upper()) # position of column on which group by is applied
            else:
                temp=dis_list[0]
                pos=col_list.index(dis_list[1].upper())

            res_head+=q_lis[5]+','+temp    
            aggr=''   #stores the aggr func  
            for x in temp:
                if x != '(':
                    aggr+=x
                else:
                    break            
            col=temp[temp.index(x)+1].upper()  # aggr func applied on this column
            if col!='*':
                pos1=col_list.index(col)  # position of col 

            tab1=open(q_lis[3]+'.csv','r')  
            Lines= tab1.readlines()
            tuple_list =[]  # to store tuples
            val_set = OrderedSet()  # to store distinct values of col to be grouped
            for line in Lines:
                temp=re.split(',',line.strip())
                tuple_list.append(temp)
                val_set.add(temp[pos])
            
            print(res_head.lower())
            for x in val_set:
                col_list=[]  # to store values of col
                res_list=[]  # to store all matching tuples
                st=str(x)+','
                for y in tuple_list:
                    if x==y[pos]:
                        res_list.append(y)
                        col_list.append(int(y[pos1]))

                if aggr.upper()=='MAX':
                    st+=str(max(col_list))     
                elif aggr.upper()=='MIN':
                    st+=str(min(col_list))        
                elif aggr.upper()=='SUM':
                    st+=str(sum(col_list))
                elif aggr.upper()=='COUNT':
                    if col=='*':
                        st+=str(len(res_list))
                    else:
                        st+=str(len(col_list))    
                elif aggr.upper()=='AVG':
                    st+=str(sum(col_list)/len(col_list))
                else:
                    sys.exit('Aggregate function does not exist')
                print(st)       

        else:
            sys.exit('Query not supported')    


    else:
        sys.exit('Query not supported')             

                         



tab_schema = {}  # a dictionary to store schemas of tables

query = sys.argv[1]

if query[-1:]!=';':
    sys.exit('; missing at the end') 

query=query[:-1] #for erasing the ;
q_lis=[]
parsed_query = sqlparse.parse(query)[0]
for i in parsed_query:
    if str(i) != ' ':
        q_lis.append(str(i))
        
if q_lis[0].upper()!='SELECT':
    sys.exit('Not a select statement')               
leng=len(q_lis)


aggr_list=['MAX', 'MIN', 'SUM', 'COUNT', 'AVG']

read_data() 
#key_list=list(tab_schema.keys())
#val_list=list(tab_schema.values())
process_query(q_lis) 
#print(tab_schema)


            





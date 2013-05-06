# -*- coding: utf-8 -*-
"""
Author:  Zhao Shi Rong <stocksortor@gmail.com>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

"""

import sqlite3 as lite
from datetime import *
import time
from  bs4  import BeautifulSoup
import urllib2
import string
import urllib
import socket
import sys


def  getWebPageManyTimes(url, num):
	for k in range(1, num):
		user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'  
                values = { }  
                headers = { 'User-Agent' : user_agent }  
                data = urllib.urlencode(values)  
                req = urllib2.Request(url, data, headers)  
                page = urllib2.urlopen(req)
                src=page.read()
                if len(src)>500:   #有时在body 内返回空
			page.close()
			return src

        return None		




def  fetchStockDataFromWeb(url):
        
     result = []

     try:
         dto = socket.getdefaulttimeout()
         socket.setdefaulttimeout(20)
       
	 src=getWebPageManyTimes(url, 5)
         #page.close()
         # print src
         s=BeautifulSoup(src)
         mtabletag=s.find('table' ,id="FundHoldSharesTable")
         var2= mtabletag.find_all('tr')
         #print var2[0].encode('gb2312')
         #print var2[0].text.encode('gb2312')
         tempcode=var2[0].text
         cutsym='('
         tempcode2=tempcode[tempcode.find(cutsym) + 1:]
	 try:
		 print tempcode2
	 except:
		 pass
         tempcode3=tempcode2[:tempcode2.find(')')]

	 try:
		 print tempcode3
	 except:
		 pass
         #print var2[1].encode('gb2312')
         #print var2[1].text.encode('gb2312')
         for i  in  range(2,len(var2)):
       #print var2[i].encode('gb2312')
           var3=var2[i].find_all('td')
           temp = []
           temp.append(tempcode3)
           for j  in range(len(var3)):
              #print  var3[j].text
              temp.append(var3[j].text.replace(u'\t',""))

        #iteminst=StockPriceItem(temp)
              
           result.append(temp)
        #result.append(iteminst)

     except  Exception as e:
         print  e
     finally:

        # restore default

        socket.setdefaulttimeout(dto)    
     return result


def  updatestocknameInDb(dbname, codedict):
    con = None
    con = lite.connect(dbname)
    rows=[]
    with con:
       cur = con.cursor()    
       cur.execute("DROP TABLE IF EXISTS stocknamemap")
       cur.execute('create TABLE stocknamemap (stockcode text, stockname text)')
       for k, v in codedict.iteritems():
        rows.append((k, v))

       cur.executemany("INSERT INTO stocknamemap VALUES(?, ?)", rows)

    con.commit()
    con.close()    
        





def   updateIndexTosqlite(dbname ,manyrows, ct):
    con = None
    con = lite.connect(dbname)
    with con:
       cur = con.cursor()    
       #cur.execute('SELECT SQLITE_VERSION()')
       #data = cur.fetchone()
       #print "SQLite version: %s" % data
       if ct==1:
          cur.execute("DROP TABLE IF EXISTS indexquote")
          cur.execute('create table indexquote (stockcode text,tradedate Date ,startprice float ,highprice float, endprice float, lowprice float, volume bigint, totalmoney bigint,primary key(stockcode,tradedate))')
      
       else:
          try:
            cur.execute("select count(*) from sqlite_master where name='indexquote'")
            data=cur.fetchone()
            
            if (data[0]==0):
               print "\ncurrent database  has no  table indexquote , create it" 
               cur.execute('create table indexquote (stockcode text,tradedate Date ,startprice float ,highprice float, endprice float, lowprice float, volume bigint, totalmoney bigint,primary key(stockcode,tradedate))')
            cur.executemany("INSERT INTO indexquote VALUES(?, ?, ?,?,?,?,?,?)", manyrows)
          except:
            print "savee index error"  

    con.commit()
    con.close() 	    


def   saveInfotosqlite(dbname ,manyrows, ct):
    con = None
    con = lite.connect(dbname)
    with con:
       cur = con.cursor()    
       #cur.execute('SELECT SQLITE_VERSION()')
       #data = cur.fetchone()
       #print "SQLite version: %s" % data
       if ct==1:
          cur.execute("DROP TABLE IF EXISTS stockquote")
          cur.execute('create table stockquote (stockcode text,tradedate Date ,startprice float ,highprice float, endprice float, lowprice float, volume bigint, totalmoney bigint,primary key(stockcode,tradedate))')
     
       else:
          try:
            cur.executemany("INSERT INTO stockquote VALUES(?, ?, ?,?,?,?,?,?)", manyrows)
          except:
            pass 

    con.commit()


    con.close()
    	    

def  getComponentBycode(code  ):
     result = []
     codedict= { }
     firsturl="http://vip.stock.finance.sina.com.cn/corp/go.php/vII_NewestComponent/indexid/399106.phtml"
     curfirsturl=string.replace(firsturl, '399106', code)
     src=getWebPageManyTimes(curfirsturl , 5)
     s=BeautifulSoup(src)
     mtabletag=s.find('table' ,id="NewStockTable")
   

     #以下计算出总的page
     allpagenum=mtabletag.findNextSibling('table')
     addresstag=allpagenum.find_all('a')
     startindex=addresstag[1]['href'].find('page=')
     secstr='&indexid=399106'
     cursecstr=string.replace(secstr, '399106', code)
     endindex=addresstag[1]['href'].find(cursecstr)
     totalpage=addresstag[1]['href'][startindex+5:endindex]
     #计算page结束

     thirdurl='http://vip.stock.finance.sina.com.cn/corp/view/vII_NewestComponent.php?page=2&indexid=399106'
     curthirdurl= string.replace(thirdurl, '399106', code)
     for  pagenum in range(1, int(totalpage)+1):
         currapstr='page='+str(pagenum)
         print currapstr
         cururl=string.replace(curthirdurl,'page=2',currapstr)
        
	 src=getWebPageManyTimes(cururl, 5)
       
         s=BeautifulSoup(src)
         mtabletag=s.find('table' ,id="NewStockTable")
     
         var2= mtabletag.find_all('tr')
         for i  in  range(2,len(var2)):
       
            var3=var2[i].find_all('td')
	    try:
		    print var3[1].text
	    except:
		    pass
            
            result.append(var3[0].text)
            codedict[var3[0].text]=var3[1].text
        

    
     return codedict   	    

def  GetNewestquoteInfoFromWeb(  ):
     cururl='http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/000001/type/S.phtml'
     testresult=fetchStockDataFromWeb(cururl)
     #print 'hello'
     print testresult
     if (testresult!=None):
         t = time.strptime(testresult[0][1], "\n\n%Y-%m-%d\n")
         d=date(t[0],t[1],t[2])
         return d
     else:
         return None 
 



def  getAllStockCode( ):
	#ressh=getComponentBycode('000001'  )
        #ressz=getComponentBycode('399106'  )
	res300=getComponentBycode('000300'  )
	resall={}
	#resall.update(ressh)
	#resall.update(ressz)
	resall.update(res300)
	return resall

def onlybuildindex(dbname, fromyear,fullnew):
    webnewday=GetNewestquoteInfoFromWeb(  )
    curSeason=(webnewday.month+2)/3
    testresult=[]

    indexurl='http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/000001/type/S.phtml?year=2012&jidu=2'
    updateIndexTosqlite(dbname ,testresult, 1)   
    for ynum in range(fromyear, webnewday.year+1):
            for cju in range(1, 5):
                if (ynum==webnewday.year) and (cju>curSeason):
                    continue
                jidustr='jidu='+str(cju)
                ystr='year='+str(ynum)
                oneurl=string.replace(indexurl, 'year=2012',ystr)
                tmpurl=string.replace(oneurl, 'jidu=2',jidustr)
                #cururl=string.replace(tmpurl, '600252', str(code))
                testresult=fetchStockDataFromWeb(tmpurl)
                #kt=saveInfotosqlite(dbname,testresult,0)
                #insertIndexQuoteToSqliteData(curconn, testresult)
                for  member in testresult:
                  strlist=member[1].split('-')
                  member[1]=date((int)(strlist[0]), (int)(strlist[1]), (int)(strlist[2]))
                updateIndexTosqlite(dbname ,testresult, 0)    








def buildstockdatabase(dbname, fromyear,fullnew):
    genurl='http://money.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/600252.phtml?year=2012&jidu=2'
    allcodes=getAllStockCode( )
    print len(allcodes)
    if fullnew==1:
	    kt=saveInfotosqlite(dbname, None,1)
    else:
            kt=saveInfotosqlite(dbname, None,0)
    updatestocknameInDb(dbname, allcodes)
   




    webnewday=GetNewestquoteInfoFromWeb(  )
    curSeason=(webnewday.month+2)/3


    for code in allcodes.keys():
        for ynum in range(fromyear, webnewday.year+1):
            for cju in range(1, 5):
                if (ynum==webnewday.year) and (cju>curSeason):
                    continue
                jidustr='jidu='+str(cju)
                ystr='year='+str(ynum)
                oneurl=string.replace(genurl, 'year=2012',ystr)
                tmpurl=string.replace(oneurl, 'jidu=2',jidustr)
                cururl=string.replace(tmpurl, '600252', str(code))
                testresult=fetchStockDataFromWeb(cururl)
                for  member in testresult:
                    strlist=member[1].split('-')
                    #for stre in strlist:
                    #    print stre
            
                    member[1]=date((int)(strlist[0]), (int)(strlist[1]), (int)(strlist[2]))
               #print member[1]
                kt=saveInfotosqlite(dbname,testresult,0)
                #kt=saveInfotosqlite(dbname,testresult,0)
                #insertsqliteData(curconn, testresult)

    indexurl='http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/000001/type/S.phtml?year=2012&jidu=2'
    testresult=[]
    updateIndexTosqlite(dbname ,testresult, 1)   
    for ynum in range(fromyear, webnewday.year+1):
            for cju in range(1, 5):
                if (ynum==webnewday.year) and (cju>curSeason):
                    continue
                jidustr='jidu='+str(cju)
                ystr='year='+str(ynum)
                oneurl=string.replace(indexurl, 'year=2012',ystr)
                tmpurl=string.replace(oneurl, 'jidu=2',jidustr)
                #cururl=string.replace(tmpurl, '600252', str(code))
                testresult=fetchStockDataFromWeb(tmpurl)
                #kt=saveInfotosqlite(dbname,testresult,0)
                #insertIndexQuoteToSqliteData(curconn, testresult)
                for  member in testresult:
                  strlist=member[1].split('-')
                  member[1]=date((int)(strlist[0]), (int)(strlist[1]), (int)(strlist[2]))
                updateIndexTosqlite(dbname ,testresult, 0)    




if  __name__=="__main__":
        if (len(sys.argv)<3):
                   print "\n Please enter DBname and from year as:  python buildstockdb.py build dbname fromyear"
                   print " example:  python buildstockdb.py build mydb.db  2013"
                   sys.exit() 
	if len(sys.argv)>3 and (sys.argv[1]=='build'):
           if (len(sys.argv)<4):
		   print "\n Please enter DBname and from year as:  python buildstockdb.py build dbname fromyear"
		   print " example:  python buildstockdb.py build mydb.db  2013"
                   sys.exit()
           fromyear=int(sys.argv[3])    
           buildstockdatabase(str(sys.argv[2]),fromyear,1 )
           sys.exit()

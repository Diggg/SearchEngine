from urllib import request
import urllib
import traceback
from bs4 import *
import sqlite3 as sqlite
import re
ignorewords=set(['the','of','to','and','a','in','is','it'])

class crawler:
    def __init__(self,dbname):
        self.con=sqlite.connect(dbname)


    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def getentryid(self,table,field,value,createnew=True):
        cur=self.con.execute(
            "select rowid from %s where %s='%s'" %(table,field,value)
        )
        res=cur.fetchone()
        if res==None:
            cur=self.con.execute(
                "insert into %s (%s) values ('%s')" % (table,field,value)
            )
            return cur.lastrowid
        else:
            return res[0]

    def addtoindex(self,url,soup):
        if self.isindexed(url): return
        print('Indexing %s' % url)

        #获取每个单词
        text=self.gettextonly(soup)
        words=self.separatewords(text)

        #得到URL的id
        urlid=self.getentryid('urllist','url',url)

        #将每个单词与url关联
        for i in range(len(words)):
            word=words[i]
            if word in ignorewords: continue
            wordid=self.getentryid('wordlist','word',word)
            self.con.execute("insert into wordlocation(urlid,wordid,location) values(%d,%d,%d)" %(urlid,wordid,i))


    def gettextonly(self,soup):
        v=soup.string
        if v==None:
            c=soup.contents
            resulttext=''
            for t in c:
                subtext=self.gettextonly(t)
                resulttext+=subtext+'\n'
            return resulttext
        else:
            return v.strip()

    def separatewords(self,text):
        splitter=re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s!='']

    def isindexed(self,url):
        u=self.con.execute(
            "select rowid from urllist where url='%s'" % url
        ).fetchone()
        if u!=None:
            #检查是否已经被建立过索引
            v=self.con.execute(
                "select * from wordlocation where urlid=%d" % u[0]
            ).fetchone()
        return False

    def addlinkref(self,urlFrom,urlTo,linkText):
        pass

    def crawl(self,pages,depth=2):
        pass

    #使用IF NOT EXISTS避免表已经存在
    def createindextables(self):
        self.con.execute('create table IF NOT EXISTS urllist(url)')
        self.con.execute('create table IF NOT EXISTS wordlist(word)')
        self.con.execute('create table IF NOT EXISTS wordlocation(urlid,wordid,location)')
        self.con.execute('create table IF NOT EXISTS link(fromid integer,toid integer)')
        self.con.execute('create table IF NOT EXISTS linkwords(wordid,linkid)')
        self.con.execute('create index IF NOT EXISTS wordidx on wordlist(word)')
        self.con.execute('create index IF NOT EXISTS urlidx on urllist(url)')
        self.con.execute('create index IF NOT EXISTS wordurlidx on wordlocation(wordid)')
        self.con.execute('create index IF NOT EXISTS urltoidx on link(toid)')
        self.con.execute('create index IF NOT EXISTS urlfromidx on link(fromid)')
        self.dbcommit()

    def crawl(self,pages,depth=2):
        headers = { 'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)' }
        for i in range(depth):
            newpages=set()
            for page in pages:
                try:
                    print('Tring to open: %s' % page)
                    req = urllib.request.Request(url=page, headers=headers)
                    c=urllib.request.urlopen(req).read()
                except Exception as err:
                    print(err)
                    traceback.print_exc()
                    print("Could not open %s" % page)
                    continue
                soup=BeautifulSoup(c,"html.parser")
                self.addtoindex(page,soup)

                links=soup('a')
                #抓取网页内的所有连接
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url=urllib.parse.urljoin(page,link['href'])
                        if url.find("'") !=-1: continue
                        url=url.split('#')[0]   #去除位置部分
                        if url[0:4]=='http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText=self.gettextonly(link)
                        self.addlinkref(page,url,linkText)
                self.dbcommit()
            pages=newpages



if "__name__" =="__main__":
    pagelist=['http://www.itpub.net/forum-2-1.html']
    crawler=crawler('searchindex.db')
    crawler.createindextables()
    crawler.crawl(pagelist)

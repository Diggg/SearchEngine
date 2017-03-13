import os
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
        path = file_root + url.split('/')[-1]
        print(path)
        #print(soup.string)
        #if not os.path.exists(path):
        #    with open(path,'w',encoding='utf-8') as f:
        #        f.write(soup.get_text())
        #        f.close()
        with open(path,'wb') as f:
            f.write(soup.prettify(encoding='utf-8'))
            f.close()
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
                    #print('Tring to open: %s' % page)
                    req = urllib.request.Request(url=page, headers=headers)
                    c=urllib.request.urlopen(req).read()
                except Exception as err:
                    print(err)
                    traceback.print_exc()
                    print("Could not open %s" % page)
                    continue
                soup=BeautifulSoup(c,"html.parser")
                self.addtoindex(page,soup)

                links=soup('a',{"class","liblink1"})
                #抓取网页内的所有连接 limit max_nums
                for link in links[:max_nums]:
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

    def calculatepagerank(self,iterations=20):
        #清除当前的PageRank表
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key,score)')
        self.dbcommit()

        for i in range(iterations):
            print("Iteration %d" % i)

            for(urlid,) in self.con.execute('select rowid from urllist'):
                pr=0.15

                #循环遍历指向当前网页的所有其他网页
                for(linker,) in self.con.execute(
                    'select distinct fromid from link where toid=%d' %urlid
                ):
                    #得到链接源对应网页的PageRank值
                    linkingpr=self.con.execute(
                        'select score from pagerank where urlid=%d' % linker
                    ).fetchone()[0]

                    #根据连接源,求得总的链接数
                    linkingcount=self.con.execute(
                        'select count(*) from link where fromid=%d' % linker
                    ).fetchone()[0]

                    pr+=0.85*(linkingpr/linkingcount)
                self.con.execute(
                    'update pagerank set score=%f where urlid=%d' % (pr,urlid)
                )
            self.dbcommit()


class searcher:
    def __init__(self,dbname):
        self.con=sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def getmatchrows(self,q):
        #构造查询的字符串
        fieldlist='w0.urlid'
        tablelist=''
        clauselist=''
        wordids=[]
        #根据空格拆分单词
        words=q.split(' ')
        tablenumber=0

        for word in words:
            #获取单词的ID
            wordrow=self.con.execute(
                "select rowid from wordlist where word='%s'" % word
            ).fetchone()
            if wordrow!=None:
                wordid=wordrow[0]
                wordids.append(wordid)

                if tablenumber>0:
                    tablelist+=','
                    clauselist+=' and '
                    clauselist+='w%d.urlid=w%d.urlid and ' % (tablenumber-1,tablenumber)
                fieldlist+=',w%d.location' % tablenumber
                tablelist+='wordlocation w%d' % tablenumber
                clauselist+='w%d.wordid=%d' % (tablenumber,wordid)
                tablenumber+=1

        #根据各个组分，建立查询
        fullquery='select %s from %s where %s' %(fieldlist,tablelist,clauselist)
        #print(fullquery)
        cur=self.con.execute(fullquery)
        rows=[row for row in cur]
        #rows = [urlid,wordlocation***] 每个urlid内每个查询单词的位置
        return rows,wordids

    # def getscoredlist(self,rows,wordids):
    #     totalscores=dict([(rows[0],0) for row in rows])
    #
    #     #此处是稍后放置评价函数的地方
    #
    #     weights=[]
    #     for (weight,scores) in weights:
    #         for url in totalscores:
    #             totalscores[url]+=weight*scores[url]
    #
    #     return totalscores

    def geturlname(self,id):
        return self.con.execute(
            "select url from urllist where rowid=%d" % id
        ).fetchone()[0]

    def query(self,q):
        rows,wordids=self.getmatchrows(q)
        scores=self.getscoredlist(rows,wordids)
        rankedscores=sorted([(score,url) for (url,score) in scores.items()],reversed=1)
        for (score,urlid) in rankedscores[0:10]:
            print('%f\t%s' % (score,self.geturlname(urlid)))

    def getscoredlist(self,rows,wordids):
        totalscores=dict([(row[0],0) for row in rows])
        #搜索权重
        #weights=[(1.0,self.frequencyscore(rows))]
        #weights=[(1.0,self.locationscore(rows))]
        weights=[(1.0,self.frequencyscore(rows)),
                 (1.5,self.locationscore(rows)),
                 (2,self.distancescore(rows))]
        for (weight,scores) in weights:
            for url in totalscores:
                totalscores[url]+=weight*scores[url]

        return totalscores

    def getrulname(self,id):
        return self.con.execute(
            "select url from urllist where rowid=%d" % id
        ).fetchone()[0]

    def query(self,q):
        rows,wordids=self.getmatchrows(q)
        scores=self.getscoredlist(rows,wordids)
        rankedscores=sorted([(score,url) for (url,score) in scores.items()],reverse=1)
        for (score,urlid) in rankedscores[0:10]:
            print('%f\t%s' % (score,self.geturlname(urlid)))

    def normalizescores(self,scores,smallIsBetter=0):
        vsmall=0.00001 #避免被0整除
        if smallIsBetter:
            minscore=min(scores.values())
            return dict([(u,float(minscore)/max(vsmall,x)) for (u,x) in scores.items()])
        else:
            maxscore=max(scores.values())
            if maxscore==0: maxscore=vsmall
            return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])
    #单词频度 计算分数
    def frequencyscore(self,rows):
        counts=dict([(row[0],0) for row in rows])
        for row in rows: counts[row[0]]+=1
        return self.normalizescores(counts)

    def locationscore(self,rows):
        locations=dict([(row[0],1000000) for row in rows])
        print(rows)
        print(locations)
        # 此处只是简单把单词在页面中的位置加总取最小值来判断其最匹配
        # 如果存在1000，1001和100，500这种情况，会把后一种情况视为更匹配，不合理
        # 可以考虑单词顺序，和单词位置差,看 distancescore()函数
        for row in rows:
            loc=sum(row[1:])
            if loc<locations[row[0]]: locations[row[0]]=loc
        print(locations)
        return self.normalizescores(locations,smallIsBetter=1)

    def distancescore(self,rows):
        #如果仅有一个单词，得分都一样
        if len(rows[0]) <= 2: return dict([(row[0],1.0) for row in rows])

        #初始化字典，并填入一个很大的数
        mindistance=dict([(row[0],1000000) for row in rows])
        for row in rows:
            dist=sum([abs(row[i]-row[i-1]) for i in range(2,len(row))])
            if dist<mindistance[row[0]]: mindistance[row[0]]=dist
        return self.normalizescores(mindistance,smallIsBetter=1)

    def inboundlinkscore(self,rows):
        uniqueurls=set([row[0] for row in rows])
        inboundcount=dict([(u,self.con.execute(
            'select count(*) from link where toid=%d' % u
        ).fetchone()[0]) for u in uniqueurls])
        return self.normalizescores(inboundcount)



if __name__ =="__main__":
    pagelist=['http://psoug.org/reference/library.html']
    crawler1=crawler('searchindex.db')
    #global file_root,max_nums
    file_root="D://GitHub//SearchEngine//htmls//"
    max_nums=10
    if not os.path.exists(file_root):
        os.mkdir(file_root)
    crawler1.createindextables()
    #crawler.crawl(pagelist)
    e=searcher('searchindex.db')
    ##Use Lower Case Character
    #rows,wordids=e.getmatchrows('oracle')
    #print(rows)
    #e.query('one table')

    faa=crawler('searchindex.db')
    faa.calculatepagerank()
    cur=faa.con.execute(
        "select * from pagerank order by score desc"
    )
    for i in cur:
        print(i)


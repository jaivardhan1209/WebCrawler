# -*- coding: utf-8 -*-
"""
Created on Sun Mar 27 21:23:33 2019

@author: jvard
"""
import urllib
from urllib.error import HTTPError, URLError
import urllib.robotparser
import socket
from bs4 import BeautifulSoup
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize as tk
import csv
import itertools
import pandas as pd
import sys
import time
import random
import re


ps = PorterStemmer()
urlDict = {}
docID = 1
f = open("Output.txt","w")
gDepth = int(sys.argv[1])
crawlDelay = str(sys.argv[2])
loc_urllist = str(sys.argv[3])
loc_urllistDict = str(sys.argv[4])

### for capturing the log in verbose
def printOut(consoleOut, string, EOL="\n"):
    
    f = open("Output.txt","a",encoding="utf-8")
    if consoleOut:
        print(string,end=EOL) 
        try: 
            f.write(string+EOL)
        except:
            pass
    f.close()

def getURLSource(string):
    try:
        return string[:string.find('/',8)+1]  
    except:
        return printOut(True,"Unexpected error in getSourceURL: "+ str(sys.exc_info()))  

def getParentIDList(dictionary,term): 
    try:
        return dictionary[term]["PARENTID"]
    except:
        return printOut(True,"Unexpected error in getParentIDList: "+ str(sys.exc_info()))  

def getMaxDocID(urlListCsvDf,urlListDict):
    if len(urlListCsvDf) > 0:
        docIDCsv = urlListCsvDf['DOCID'].max()+1
    else:
        docIDCsv = 1
    
    if len(urlListDict) > 0:
        docIDDict = max(int(d['DOCID']) for d in urlListDict.values()) + 1
    else:
        docIDDict = 1
        
    return max(docIDCsv,docIDDict) 

def findUrls(parentUrl,UrlArr,depth,urllistcsv):

    ## BAse condition to return the recursive call
    if depth == gDepth:
        return
    
    printOut(True,"Depth: "+str(depth))
    links = []
    childUrl = []
    
    if crawlDelay.upper() == 'R':
        sleepTimer = random.randint(1,10)
    elif crawlDelay.isnumeric:
        sleepTimer = int(crawlDelay)
    else:
        sleepTimer = 0
        
    if depth == gDepth:
        return

    docID = getMaxDocID(urllistcsv,urlDict)    
        
    if len(UrlArr) == 0:
        UrlArr.append(parentUrl)
        #parentUrlCheck = True
    
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(getURLSource(parentUrl)+"robots.txt")
    rp.read()
    
    for idx,url in enumerate(UrlArr):   
        printOut(True,str(depth)+" : "+str(idx+1)+'/'+str(len(UrlArr))+" : "+str(url))
        
        try:
            parentUrlId = urllistcsv[(urllistcsv['URL']==url)].values[0][2]
            parentUrl = urllistcsv[(urllistcsv['DOCID']==parentUrlId)].values[0][0]
            if (parentUrl in urllistcsv['URL'] and urllistcsv[(urllistcsv['URL']==parentUrl)].values[0][4] == True
               and urllistcsv[(urllistcsv['URL']==parentUrl)].values[0][5] == False):
                continue
        except:
        
            try:
                if urlDict[url]['SPAM'] == False and urlDict[url]['PARSEDFQ'] == False:
                    time.sleep(sleepTimer)
                    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7); Jai/AcademicProjectUTD'
                    headers={'User-Agent':user_agent} 
                    
                    if rp.can_fetch("*", url):
                        pass
                        print("pass")
                    else:
                        printOut(True,'Disallowed by robots.txt')
                        continue
                    
                    try:
                        urllib.request.urlcleanup()
                        request=urllib.request.Request(url,None,headers)
                        handle = urllib.request.urlopen(request,timeout=10)
                        
                    except HTTPError as error:
                        printOut(True,"HTTPError - "+str(error.reason))
                        continue
                    except URLError as error:
                        if isinstance(error.reason, socket.timeout):
                            printOut(True,"URLError - "+str(error.reason))
                        else:
                            printOut(True,"URLError - "+str(error.reason))
                        continue
                    
                    urlDict[url]['PARSEDFQ'] = True
                    html_gunk =  handle.read()
                    soup = BeautifulSoup(html_gunk, 'html.parser')
                    links = list(soup.find_all('a', href=True))
                    createContentFiles(soup,url,urlDict)
                    urlDict[url]['PARSEDFC'] = True
                    printOut(True,str(urlDict[url]["DOCID"]) + " - " +url)
                    
                    if gDepth - depth != 1:
                        urlArr = []
                        for urlLinks in links:
                            urlArr.append(str(urlLinks['href']).strip())
                            
                        
                        for link in set(urlArr):
                            
                            parentIdArr = []
                            parentIdArr.append(urlDict[url]['DOCID'])
                            
                            if len(link) == 0:
                                continue
                            if (link[0] != '#' and link[:4] != 'mail' and link[-4:] != '.pdf' and 'javascript' not in link):
                               
                                if link[:4] == 'http':
                                    newLink = link
                                elif link[:5] == '//www':
                                    newLink = 'https:'+link
                                elif link[:3] == '../':
                                    for m in re.finditer("\.\./",link):
                                        x = m.end(0)
                                    newLink = getURLSource(url)+link[x:]   
                                elif link[:1] == '/':
                                    newLink = getURLSource(url)+link[1:]
                                else:
                                    newLink = getURLSource(url)+link
                                
                                if newLink not in urlDict.keys():
                                    if getURLSource(newLink) == getURLSource(url):
                                        urlDict[newLink] = {'DOCID':docID, 'PARENTID':parentIdArr, 'PARSEDFQ':False,'PARSEDFC':False, 'SPAM':False}
                                        childUrl.append(newLink)
                                    else:
                                        urlDict[newLink] = {'DOCID':docID, 'PARENTID':parentIdArr, 'PARSEDFQ':False,'PARSEDFC':False, 'SPAM':True}
                                    docID += 1
                                   
                                else:
    
                                    try:
                                        parentIdArr = getParentIDList(urlDict,newLink)
    
                                        if urlDict[url]['DOCID'] != urlDict[newLink]['DOCID']:
                                            parentIdArr.append(urlDict[url]['DOCID'])
                                            
                                        if urlDict[url]['DOCID'] not in parentIdArr:
                                            urlDict[url].update({'PARENTID':parentIdArr})
                                    except:
                                        printOut(True,"Error in else - "+ str(sys.exc_info()[0]))
    
            except:
                printOut(True,"Unable to open - "+url)
                printOut(True,"Unexpected error: "+ str(sys.exc_info()))
                continue
                
            
    ### recursively call the URL to further depth 
    return findUrls(parentUrl,childUrl,depth+1,urllistcsv)


def createUrlDict(line,urlListCsv):

    
    printOut(True,"in createUrlDict: "+line)

    docID = getMaxDocID(urlListCsv,urlDict)
    string = line.replace('\n','').strip()
    urlDict[string] = {'DOCID':docID, 'PARENTID':[-1], 'PARSEDFQ':False, 'PARSEDFC':False, 'SPAM':False}
    findUrls(string,[],0,urlListCsv)
            
    return urlDict

def createContentFiles(soupData,url,urlDict):
       
    paragraphs = soupData.find_all('p')
    titles = soupData.find_all('title')
    h1 = soupData.find_all('h1')
    h2 = soupData.find_all('h2')
    filename = str(urlDict[url]["DOCID"])
    file = open('Content/'+filename,"w",encoding='utf-8')
    metaWords = createMetadataFiles(soupData,url,urlDict)
    for word in metaWords:
        file.write(ps.stem(word)+"\n")
    for j in range(len(titles)):
        line = tk(titles[j].get_text())
        for word in line:
            file.write(ps.stem(word)+"\n")
    for k in range(len(h1)):
        line = tk(h1[k].get_text())
        for word in line:
            file.write(ps.stem(word)+"\n")
    for l in range(len(h2)):
        line = tk(h2[l].get_text())
        for word in line:
            file.write(ps.stem(word)+"\n")
    for i in range(len(paragraphs)):
        line = tk(paragraphs[i].get_text())
        for word in line:
            file.write(ps.stem(word)+"\n")
            
    file.close()

def createMetadataFiles(soupData,url,urlDict):

    line = []
    wordArr = []
    for tag in soupData.find_all("meta"):
        if tag.get("property", None) == "og:title":
            line.append(tk(tag.get("content", None)))        
        if tag.get("name",None) == "keywords":
            line.append(tk(tag.get("content", None)))
        if tag.get("name",None) == "description":
            line.append(tk(tag.get("content", None)))    
    for word in list(itertools.chain.from_iterable(line)):
        wordArr.append(ps.stem(word))
 
    return wordArr
   
       
def crawler():
    
    
    urlFilePath = loc_urllist
    printOut(True,"Creating URL Dictionary...")
    global urlDict
    with open(urlFilePath) as urlfile:
        for line in urlfile:
            urllistcsv = pd.read_csv(loc_urllistDict,encoding="ISO-8859-1")    
            line = line.strip()
            try:
                if line in urllistcsv['URL'].get_values() and urllistcsv[(urllistcsv['URL']==line)].values[0][3] == True:
                    continue
            except:
                pass
            tempUrlDict = createUrlDict(line, urllistcsv)
            with open(loc_urllistDict,'a',newline='',encoding="utf-8") as urlCsv:
                file2 = csv.writer(urlCsv)
                for url in tempUrlDict.keys():
                    file2.writerow([url,str(tempUrlDict[url]["DOCID"]),str(set(tempUrlDict[url]["PARENTID"])),str(tempUrlDict[url]["PARSEDFQ"]),str(tempUrlDict[url]["PARSEDFC"]),str(tempUrlDict[url]["SPAM"])])
            
            urlDict = {}    
            
    printOut(True,"Created URL Dictionary...")

    
if __name__ == '__main__':
    
    crawler()       

         
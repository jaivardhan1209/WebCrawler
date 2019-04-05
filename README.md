# WebCrawler
This is custom python code to crawl the website
It takes in the following parameters -

<dfs depth> <delay (either 'r' or any number)> <'urllist.txt' path> <'urllistDict.csv' path>

Depth : defines how deep the crawler will go.
Delay: it will add delay for subsequent call.
urllist: this is exhaustive list of all the url to parse.
urllistDict: This structure keep track of parsed url so that after visit mark the url as visited.

---------------- How to keep the crawling on track ------------
I tried to parse the url till first occurance of / . and then match with the child url or visiting url. If the prefix of parent
and child url is different then it might lead to unrelated page and kill our computation process. So I discard all those children.

Algorithm: 
Striaght forward implementation of DFS where my urllist is server as the root of the n array graph. and with given depth we can control
the depth of the search space. 

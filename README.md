# pixiv-ugoira-scrape-bot

warning: massive data involved  
prerequisites:  
```
pixivpy
pillow
sqlq
threadwrapper
```
for index.py cgi:
```
pythoncgi
omnitools
```

## crawler

job: find all illust id by users who posts ugoira
```python
test.py crawl
```

## downloader

job: download ugoira metadata and zips, convert them into OPTIMIZED gifs
```python
test.py download
```

## cache

job: cache ugoira gifs paths for cgi
```python
test.py cache
```

# notes on execution

the first `test.py` instance will create a sql execution queue server.  
if `crawler` started first and then `downloader` is started,  
when you terminate the `crawler`, the `downloader` will explode  
because there is no connection to the sql execution queue server.

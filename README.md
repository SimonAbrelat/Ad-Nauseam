# Ad-Nauseam
An minimal, open-source Wikipedia crawler built for CS4675

## Description
This is meant to be a the best naive solution that I could make to this project. Everything is within 1 file.
There is a main ```CrawlerManager``` class which has 4 simple functions:
1. add => Adds URLS to the list that needs to be visited
2. pop => Gets a URLS from that list
3. record => Records URL, hash, and keywords in a database
4. run => Let's the spiders hatch

## How to Run
```pip3 -r requirements.txt```
```python3 main.py``` ;)
Right now all of the customization is in the code, but it is simple to understand. 
It does not output anything to console, only the files. Very very simple.
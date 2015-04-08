# minisql

Implementation of Basic SQL operators - A Database Systems assignment

````
python minisql.py members.csv members2.csv
````
### Query Examples

1. **PROJECTION**: ````SELECT First,Last from members````
2. **SELECTION**: ````SELECT First,Website from members WHERE First = Adam````
3. **UNION**: ````SELECT First,Last from members UNION SELECT First,Last from members2````
4. **MINUS**: ````SELECT First,Facebook from members MINUS SELECT First,Facebook from members2````
5. **CROSS**: ````SELECT First,Last from members CROSS SELECT First,Last from members2````

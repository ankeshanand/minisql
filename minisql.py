__author__ = 'ankesh'
import sys
import csv

class Clause():
    def __init__(self):
        self.operand1 = ''
        self.operator = ''
        self.operand2 = ''

class DBTable():
    def __init__(self):
        self.columnNames = []
        self.records = []

    def projection(self, columns):
        try:
            for col in columns:
                if col not in self.columnNames:
                    raise ValueError('Invalid column name. Exiting.')
        except ValueError as e:
            print e
            sys.exit(1)
        result = DBTable()
        result.columnNames = columns
        for rec in self.records:
            result_rec = {}
            for col in columns:
                result_rec[col] = rec[col]
            result.records.append(result_rec)
        return result

    def union(self, table2):
        """
        Perform a union of two tables, i.e.
        the set of tuples that are either in self, or in Table2 or both.
        The arity of both the tables should be same.
        Column headers do not matter.
        :param table2: A DBTable
        :return:
        """

        # check if arity of both the tables are same
        if len(self.columnNames) != len(table2.columnNames):
            print 'Arity of the tables are not same. Cant perform union.'
            return

        results = set([])
        for rec in self.records:
            rec_tuple = tuple([v for (k, v) in rec.items()])
            results.add(rec_tuple)
        for rec in table2.records:
            rec_tuple = tuple([v for (k, v) in rec.items()])
            if rec_tuple not in results:
                results.add(rec_tuple)
        for item in results:
            print item

    def setDifference(self, table2):
        """
        Prints the set of tuples that are in self but not in Table2.
        Arity of both the tables should be same.
        :param table2: A DBTable
        :return:
        """
        results = set([])
        for rec in self.records:
            rec_tuple = tuple([v for (k, v) in rec.items()])
            results.add(rec_tuple)
        for rec in table2.records:
            rec_tuple = tuple([v for (k, v) in rec.items()])
            if rec_tuple in results:
                results.remove(rec_tuple)
        for item in results:
            print item

    def cartesianProduct(self, table2):
        """
        Prints a cartesian product of self and Table2.
        :param table2: A DBTable
        :return:
        """
        results = set([])
        for rec1 in self.records:
            rec_list1 = [v for (k, v) in rec1.items()]
            for rec2 in table2.records:
                rec_list2 = [v for (k, v) in rec2.items()]
                rec_tuple = tuple(rec_list1 + rec_list2)
                results.add(rec_tuple)

        print str(len(results))
        for item in results:
            print item

    def printTable(self, columns=True):
        if columns:
            line_len = 0
            for col in self.columnNames:
                line_len += len(col)
                print col + '|',
            print
            print '-' * line_len
            for rec in self.records:
                for col in self.columnNames:
                    print rec[col] + '|',
                print

    def selection(self,clause):
        result = DBTable()
        result.columnNames = self.columnNames
        if clause.operator == '=':
            for rec in self.records:
                if rec[clause.operand1] == clause.operand2:
                    result.records.append(rec)
        return result


tables = []
tables_dict = {}

def process_query(query):
    tokens = query.split(' ')
    keywords = ['UNION','MINUS','CROSS']
    if any(x in query for x in keywords):
        if 'UNION' in tokens:
            for i,token in enumerate(tokens):
                if token == 'UNION':
                    table1 = process_query(' '.join(tokens[:i]))
                    table2 = process_query(' '.join(tokens[i+1:]))
                    table1.union(table2)
                    result = DBTable()
                    return result
        elif 'MINUS' in tokens:
            for i,token in enumerate(tokens):
                if token == 'MINUS':
                    table1 = process_query(' '.join(tokens[:i]))
                    table2 = process_query(' '.join(tokens[i+1:]))
                    table1.setDifference(table2)
                    result = DBTable()
                    return result
        elif 'CROSS' in tokens:
            for i,token in enumerate(tokens):
                if token == 'CROSS':
                    table1 = process_query(' '.join(tokens[:i]))
                    table2 = process_query(' '.join(tokens[i+1:]))
                    table1.cartesianProduct(table2)
                    result = DBTable()
                    return result

                    
    else:
        cols = tokens[1]
        table_no = tables_dict[tokens[3]]
        if cols == '*':
            cols = tables[table_no].columnNames
        else:
            cols = cols.split(',')
        if 'WHERE' in tokens:
            for i,t in enumerate(tokens):
                if t == 'WHERE':
                    c = Clause()
                    c.operand1 = tokens[i+1]
                    c.operator = tokens[i+2]
                    c.operand2 = tokens[i+3]
                    result = tables[table_no].selection(c)
                    return result.projection(cols)
        return tables[table_no].projection(cols)


if __name__ == '__main__':
    files = ['data/'+x for x in sys.argv]
    del files[0]
    tables = [DBTable() for f in files]
    for i in range(1, len(sys.argv)):
        dot_pos = sys.argv[i].find('.')
        table_name = sys.argv[i][:dot_pos]
        tables_dict[table_name] = i-1
    for i, file in enumerate(files):
        with open(file, 'rb') as f:
            reader = csv.reader(f)
            for j, row in enumerate(reader):
                if j == 0:
                    tables[i].columnNames = row
                else:
                    rec = {}
                    for n, col in enumerate(tables[i].columnNames):
                        rec[col] = row[n]
                    tables[i].records.append(rec)
        print
        print "Printing Table " + str(i+1)
        tables[i].printTable()
            # tables[i].select(['First', 'Last'])
    # tables[0].union(tables[1])
    # tables[0].setDifference(tables[1])
    # tables[0].cartesianProduct(tables[1])
    while(True):
        print '>',
        query = raw_input()
        result = process_query(query)
        result.printTable()

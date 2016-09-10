'''
Do not have clear thinking. Can't check result in time because of the RDD operation.
'''
from pyspark import SparkContext
import copy
MIN_SUP = 2
class Node(object):
    def __init__(self,val,sup,parent,children):
        self.val = val
        self.sup = sup
        self.parent = parent
        self.children = children

#sort a transaction according to sorted frequent 1-itemset
def sort_transaction(transaction, tableWithPointer):
    sorted_transaction = []
    for item in tableWithPointer:
        if item[0] in transaction:
            sorted_transaction.append(item[0])
    return sorted_transaction

#construct fptree
def construct_fptree(root, transaction, tableWithPointer):
    cur = root
    for item in transaction:
        cur = insert_fptree(cur, item ,tableWithPointer, 1)
    return root

#insert a node into fptree
def insert_fptree(cur,item ,tableWithPointer,freq):
    if cur.children == []:
        new_child = Node(item, freq, cur, [])
        cur.children.append(new_child)
        cur = new_child
        #add new_child into table
        for factor in tableWithPointer:
            if factor[0] == item:
                factor[2].append(new_child)
                break
    else:
        flag = False
        for child in cur.children:
            if child.val == item:
                child.sup += freq
                cur = child
                flag = True
                break
        if not flag:
            new_child = Node(item, freq, cur, [])
            cur.children.append(new_child)
            cur = new_child
            #add new_child into table
            for factor in tableWithPointer:
                if factor[0] == item:
                    factor[2].append(new_child)
                    break
    return cur

#get conditional pattern base of node
def one_path(node):
    path = []
    freq = node.sup
    if node.parent.val == None:
        return None
    else:
        while node.parent.val != None:
            path.append(node.parent.val)
            node = node.parent
    #Note: path.reverse() do not return reversed path
    path.reverse()
    return (path,freq)
def all_path(list):
    condpattBase = []
    for node in list:
        onepath = one_path(node)
        if onepath != None:
            condpattBase.append(one_path(node))
    return condpattBase

#change ([I2,I1], 1) to [(I2,1), (I1,1)]
def one_one(tup):
  list = []
  for item in tup[0]:
    list.append((item,tup[1]))
  return list
#allpath: [ ([I2,I1], 1), ([I2,I1,I3],1) ]
#caculate the sup of every item in allpath,and return table :supCount and supCountPointer
def sup_count(allpath):
    allpathRDD = sc.parallelize(allpath)
    # unstable sorting lead to some question. HeadTable is inconsistent with conditional fptree
    supCountRDD = allpathRDD.flatMap(one_one).reduceByKey(lambda x,y:x+y).filter(lambda x: x[1] >= MIN_SUP).sortBy(lambda x:x[1],ascending=False)
    supCountPointerRDD = supCountRDD.map(lambda x:(x[0],x[1],[]))
    return supCountRDD.collect(), supCountPointerRDD.collect()
#construct conditional fptree for certain postfix
def construct_cond_tree(allpath):
    root = Node(None, None, None, [])
    supCount, supCountPointer = sup_count(allpath)
    for p in allpath:
        cur = root
        freq = p[1]
        for item in p[0]:
            for tup in supCount:
                flag = False
                if item == tup[0]:
                    flag = True
                    break
            if flag:
                cur = insert_fptree(cur,item ,supCountPointer,freq)
    return root, supCountPointer

#conditional fptree contain single path or not
def isSingleBranch(supCountPointer):
    #just check the last frequent item is not reliable.
    #if len(supCountPointer[-1][2]) == 1:
    for row in supCountPointer:
        if len(row[2]) > 1:
            return False
    return True

#extract frequent pattern
def extract_pattern(postfix, allpath):
    if allpath == []:
        return
    root, supCountPointer = construct_cond_tree(allpath)
    cur = root
    itemset = []
    if isSingleBranch(supCountPointer):
        while cur.children != []:
            child = cur.children[0]
            cur = child
            itemset.append((child.val, child.sup))
        combine(itemset, postfix)
    else:
        for factor in supCountPointer:
            #generate new postfix
            itemlist = []
            itemlist.extend(postfix[0])
            itemlist.append(factor[0])
            newpostfix = (itemlist, factor[1])
            print newpostfix
            newallpath = all_path(factor[2])
            extract_pattern(newpostfix, newallpath)
        '''
        for factor in supCountPointer[::-1]:
            flag = True
            newNode = None
            for node in factor[2]:
                if node.children != []:
                    flag = False
                    break
            if flag:
                postfix.append(factor[0])
                for row in supCountPointer:
                    basepatt = (postfix,factor[1])
                    print basepatt
                newallpath = all_path(factor[2])
                extract_pattern(postfix, newallpath)
                break
        '''
    return
def combine(itemset, postfix):
    maxl = len(itemset)
    l = 0
    pattlist = []
    #1 frequent pattern
    for item in itemset:
        pattlist.append(([item[0]], item[1]))
    l+=1
    while l<maxl:
        templist = []
        for item in itemset:
            for patt in pattlist:
                # patt is tuple type, can not be modified.
                # avoid duplicate pattern, every pattern is in dictionary sort
                if item[0] > patt[0][-1]:
                    # temppatt = patt or temp = patt[0] when we modify temp, patt will be modified at the same time.
                    # so we should convert tuple to list. even if convert to list, the effect con not eliminate.
                    # how to solve this question? Use deepcopy()
                    #temppatt = copy.deepcopy(patt)# we can use another method replace
                    itemlist = []
                    # like reverse() function, append() and insert() return None
                    itemlist.extend(patt[0])
                    itemlist.insert(-1, item[0])
                    freq = item[1] if item[1] < patt[1] else patt[1]
                    templist.append((itemlist, freq))
        l += 1
        pattlist.extend(templist)
    for item in postfix[0]:
        for patt in pattlist:
            if item not in patt[0]:
                patt[0].append(item)
    print pattlist
    return pattlist


if __name__=="__main__":
    sc = SparkContext('local')
    transactionRDD = sc.textFile("C:\Users\w\Desktop\purchase.txt").map(lambda x:x.split(","))
    transactionRDD.cache()
    #print transactionRDD.collect()
    countRDD = transactionRDD.flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
    #print countRDD.collect()
    tableRDD = countRDD.filter(lambda x:x[1]>=2).sortBy(lambda x:x[1],ascending=False)
    tableRDD.cache()
    tableWithPointerRDD = tableRDD.map(lambda x:(x[0], x[1], []))
    tableWithPointer = tableWithPointerRDD.collect()
    #treeRDD = transactionRDD.map(lambda x: construct_fptree(root,sort_transaction(x,tableWithPointer), tableWithPointer))
    tablebroadcast = sc.broadcast(tableRDD.collect())
    sortedTransactionRDD = transactionRDD.map(lambda x:sort_transaction(x, tablebroadcast.value))
    #construct fptree
    root = Node(None, None, None, [])
    sortedTransactions = sortedTransactionRDD.collect()
    for t in sortedTransactions:
        construct_fptree(root, t, tableWithPointer)

    #verify the fptree has bee constructed
    for child in root.children:
        for c in child.children:
            print '{}:{} parent{}'.format(c.val,c.sup,c.parent.val)

    # what is a tree ? ? ? ? if when I construct a tree, the relevant datas are stored in memory
    treeBroadcast = sc.broadcast(root)

    #verify updated tableWithPointer
    for node in tableWithPointer[2][2]:
        print node.val,node.parent.val
    #verify the all_path function
    print all_path(tableWithPointer[2][2])
    #conditional pattern base: (I5, [ ([I2,I1], 1), ([I2,I1,I3],1) ])
    '''
    # +++++++++++++++++++++++++++Can not parallelize custom type to RDD+++++++++++++++++++++++++++++++++++++
    updatedTableWithPointerRDD = sc.parallelize(tableWithPointer)
    condPatternBaseRDD = updatedTableWithPointerRDD.map(lambda x: (x[0], all_path(x[2])))
    print condPatternBaseRDD.collect()
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


    cond_tableRDD = condPatternBaseRDD.map(lambda x:(x[0],sup_count(x[1])))
    cond_tableRDD.collect
    '''
    tableWithCondPattBase = []
    for row in tableWithPointer[::-1]:
        condPattBase = all_path(row[2])
        print condPattBase
        #Because row is a tuple,we cannot just modify row[2]. so row[2] = condPattBase is wrong
        row = (row[0],row[1],condPattBase)
        tableWithCondPattBase.append(row)

    tableWithCondPattBaseRDD = sc.parallelize(tableWithCondPattBase)
    print tableWithCondPattBaseRDD.take(3)

    #++++++++++++++++++++++++++++++++++++++debug, then will success+++++++++++++++++++++++++++++++++++++++++
    #tableWithCondPattBaseRDD.map(lambda x: extract_pattern(x[0], x[2]))
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    for row in tableWithCondPattBase:
        # extract_pattern(a, Tree) a = ([row[0]], row[1]), Tree construct from row[2]
        extract_pattern(([row[0]], row[1]), row[2])

    sc.stop()

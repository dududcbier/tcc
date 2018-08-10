from py2neo import Graph, Relationship, Node, Subgraph
from subprocess import check_output, CalledProcessError
from progress.bar import Bar
import math
import os
import pandas as pd
import gzip

FILE_META = 'meta_Books_1k.json.gz'
FILE_REVIEWS = 'Books_5_1k.json.gz'

weight = {
    'also_bought': 1,
    'also_viewed': 0.5,
    'buy_after_viewing': 0.75,
    'bought_together': 2
}

class User(Node):
    def __init__(self, name, id):
        super().__init__("User", name=name, id=id)

class Book(Node):
    def __init__(self, id, price=None, description=None):
        price, description = clean(price, description)
        super().__init__("Book", id=id, price=price, description=description)

class Reviewed(Relationship):
    def __init__(self, user, book, helpful, text, overall, summary, unixTime, time): 
        super().__init__(user, "reviewed", book, helpful=helpful, reviewText=text, overall=overall, 
                         summary=summary, unixReviewTime=unixTime, reviewTime=time, weight=1)

def parse(path):
  g = gzip.open(path, 'rb')
  for l in g:
    yield eval(l)

def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
  return pd.DataFrame.from_dict(df, orient='index')

def isnan(x):
    r = False
    try:
        r = math.isnan(x)
    except TypeError: pass
    return r

def clean(*args):
    cleaned_args = []
    for a in args:
        if isnan(a):
            cleaned_args.append(None)
        else: cleaned_args.append(a)
    return cleaned_args

def count_lines(path):
    n = 0
    try:
        n = int(check_output(['wc', '-l', path]).decode('utf-8').split()[0])
    except CalledProcessError:
        n = -1
    return n
        
db_pwd = os.environ['NEO4J_PWD']
graph = Graph(password=db_pwd)
n_books = count_lines(FILE_META[:-3])
n_reviews = count_lines(FILE_REVIEWS[:-3])

metadata_df = getDF(FILE_META)
print("Finished reading metadata file")
# Add all the books first
tx = graph.begin()
bar = Bar('Adding books', max=n_books)
n_relationships = 0
for row in metadata_df.itertuples():
    book = Book(getattr(row, "asin"), getattr(row, "price"), getattr(row, "description"))
    tx.merge(book, "Book", "id")
    related = getattr(row, "related")
    if isnan(related): 
        bar.next()
        continue
    for k in related:
        n_relationships += len(related[k])
    bar.next()
bar.finish()
tx.commit()
# Add all the relationships
tx = graph.begin()
bar = Bar('Adding relationships', max=n_relationships)
for row in metadata_df.itertuples():
    related = getattr(row, "related")
    book1 = graph.nodes.match("Book", id=getattr(row, "asin")).first()
    if isnan(related): continue
    for k in related:
        for book_id in related[k]:
            book2 = graph.nodes.match("Book", id=book_id).first()
            if book2 is not None:
                r = Relationship(book1, k, book2, weight=weight[k])
                tx.merge(r)
            bar.next()
bar.finish()
tx.commit() 
print("Finished importing books")

reviews_df = getDF(FILE_REVIEWS)
print("Finished reading reviews file")
bar = Bar('Adding users and reviews', max=n_reviews)
for row in reviews_df.itertuples():
    user = User(getattr(row, "reviewerName"), getattr(row, "reviewerID"))
    book = graph.nodes.match("Book", id=getattr(row, "asin")).first()
    reviewed = Reviewed(user, book, getattr(row, "helpful"), getattr(row, "reviewText"), 
                        getattr(row, "overall"), getattr(row, "summary"),
                        getattr(row, "unixReviewTime"), getattr(row, "reviewTime")
                    )
    tx = graph.begin()
    tx.merge(user, "User", "id")
    tx.merge(reviewed)
    tx.commit()
    bar.next()
bar.finish()
print("Finished importing users and reviews")
## Background ##

All popular databases used today are what I call "macro" databases.  This includes both relational databases and non-relational databases (or "nosql" as some call it).  The same technology is at the core of all of these databases.  They  efficiently pack row/column data in some way, and allow you to "index" that data in one or more ways (usually with b-trees).  What separates them from each other is that they each add different features on top of this core architecture (such as transactions, query languages, standalone server mode, etc), and restrict you to a subset of possible storage paradigms (such as the key-value store, which restricts you to exactly two columns with an index on one of them).

## How is StemDB different? ##

StemDB is a "micro" database.  For now it handles:

  * Storing any number of rows/columns, where you can specify for each column its data column and number of bits
    * number of bits doesn't have to be byte-aligned, so you can pack your data into 5-bit integers if it you need the space.
    * storage can be either persistent on disk or only in memory
  * Adding any number of indexes to any sequence of columns, allowing an arbitrary number of duplicates
    * most databases restrict keyed columns to either no duplicates (unique key) or infinitely many duplicates.  With StemDB you can have any arbitrary number
  * Querying for data by each key, using input data that can be a subsequence of the columns
    * If you add multiple columns to an index and only query with the first one, you get all the rows that match the first

## But more is better, right? ##

Wrong.  Usually there is some performance or efficiency cost for every feature the macro databases add.  If you don't want those features and your application is extremely performance critical, you can create what you need with StemDB and it will be much faster and much more efficient.  Sometimes the cost for the features is also to limit you to a particular data paradigm, so StemDB is also much more flexible than macro databases.

## How does it work? ##

It's an embedded database written in C that you can interact with from Python.  For indexes it uses radix trees, which are have more consistent performance than b-trees as the number of entries grows to millions and billions.  There is also never any need to "optimize" an index as with b-trees, because the radix tree does not need to be balanced.  Both indexing and querying for a key value takes O(number of bits in the key), so it's very fast.

## How fast? ##

In my testing, when the database fits into memory, I was able to insert and index over 1M rows per second into a 2-column database with 32-bit unsigned integer columns, with a single index on both columns.  This was sustained consistently over billions of rows.
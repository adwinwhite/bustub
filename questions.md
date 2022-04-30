What is frame in buffer management?
   - page in replacer is called frame I guess.

What is free list in buffer management?
   - list that contains unused pages' indices. (not page_id, just index of pages_).


How are replacer's methods called?
   - Not implemented. Up to me!

Is page_id universal or just valid in pool manager?
   - Universal. 

Why do we need an index in memory?
   key -> address of record on disk
   Only need to store keys in memory. Can hold more data.

But are btree+ used in index?
   yes. 
   it's designed to be pages thus we can save/restore index to/from disk.
   it's kind of like metadata of filesystem.
   

Obsoled pages and root_page_id_ may be used.
How to resolve this?

if you need to acquire all the locks to perform an operation, why not just lock the tree?
   it seems that I do need to lock the whole tree.
   So no concurrency at all?
      or when everyone is just reading.
   Well, I found out that you don't need to lock the whole chain, just nodes got changed.(root maybe not included)
      But until an operation is done, we can't really determine which nodes changed.
         Check size when looking for leaf page?
   I should really read the notes first.

How to record latches acquired?
   Operations in a transaction is sequential.
   So just store and remove them in transaction.
   The page set starts empty with each operation.

root page id can only be changed at two mirror places.

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

What is executor?

What is tuple?
   ([values], [columns]) serialized.

How does global depth and local depths work in extendible hashing scheme?
   global depth is the number of bits of hash used to determine unique keys. Also the number of split in directory.
   local depths are the number of split in buckets.

What does directory do?
   Use the least number of bits to keep hashs of keys unique.
   Store mappings: hash(with mask) -> bucket.

What does bucket do?
   Store kv.

How does insertion work in extendible hashing scheme?
   1. calculate hash of key.
   2. use masked hash(global depth) to find corresponding bucket.
   3. try to insert the (k, v) into bucket.
   4. if the bucket is full, 
         if local depth is equal to global depth,
            increase global depth, and make additional dir_indices point to unchanged buckets.
         if local depth is smaller than global depth which means that the bucket can split without changing global depth,
            increase local depth thus changing mask, and new a bucket pointed by once duplicate dir_index,
            redistribute (k, v)s into the two buckets according to the new mask, 
            retry insertion
      else,
         insert the pair

Do I need to redistribute kv pairs after increasing global depth since masked_key changes?
   No need since both masked_keys pointint to the same bucket.


Flags in bucket page?
   occupied - once has value. 
   readable - not be removed.


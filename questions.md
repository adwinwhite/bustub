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
   

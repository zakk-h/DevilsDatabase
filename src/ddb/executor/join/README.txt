Your team members and a brief summary of what each of you have done for this milestone.

Team members: Judy, Riya, Zakk
We all collaborated on the solution to this milestone by learning about and discussing how relevant code in the Devil's Database works and how we may learn from existing implementations for certain operators in files like bnlj.py, mergesort.py and mergeeqj.py. Regarding coding, we decided to mainly pair-program on each others' computers, discussing our approach as one person coded. At some times when we were debugging, we split off to individually troubleshoot, but we brainstormed and arrived at solutions together.
 

[Optional] A description of your code/algorithm design, especially if you have implemented any features/ideas worth highlighting.

To implement hashjoineqj.py, we first recursively hash each row in the left and right tables and store the hashed rows in left and right buckets. In executer_recurse(), we read rows from the left and right tables in blocks and hash the rows using sha256 hash function. We end our recurse function when no bucket's size, for the left and right table, exceeds the size of memory, BLOCK_SIZE*(num_memory_blocks-1). If a bucket's size does exceed the size of memory, we simply re-hash all buckets for the left and right tables by increasing the mod. Our recursive function ultimately returns a a list of buckets containing hashed rows for the right table and a list of buckets containing hashed rows for the left table. In our main execute() function, we join rows in the right table to rows in the left table by bucket by iterating through each bucket for the left table, joining each row in that bucket with the row in the right table with the matching hash code (we accomplish this with modifying the string). As we match each row, we yield the tuple yield (*rowL, *rowR), effectively having our exec() function return an generator. 

Since we are using temporary partition files to store bucketed rows, each file must be explicitly closed after use to release system resources. The with statements aid us in this goal: automatically does file opening/closing, having only one at a time over to reduce overhead. Calling flush writes the remaining data to the file, needed for the last partial block. Instead of saving all joined rows in memory, we stream them with a generator. 


[Optional] A description of known bugs or limitations, especially if your code did not pass all autograder tests.

N/A

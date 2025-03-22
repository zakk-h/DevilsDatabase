Your team members and a brief summary of what each of you have done for this milestone.

Team members: Judy, Riya, Zakk
We all collaborated on the solution to this milestone by learning about and discussing how relevant code in the Devil's Database works and how we may learn from existing implementations for certain operators. Regarding coding, we decided to mainly pair-program on each others' computers, discussing our approach as one person coded. At some times when we were debugging, we split off to individually troubleshoot, but we brainstormed and arrived at solutions together.
We had to rewrite the algorithm a few times - firstly because we were writing each group to a file, even if none of the aggregation expressions were non-incremental, so we may not have memory to do that.
Secondly, we had an aggregation expression loop on the outside, and then iterating over rows inside. We decided to restructure it such that for a given row, we would update all the aggregation expressions, and then move to the next row. Though functionality equivalent, this was a nicer way to do it.

[Optional] A description of your code/algorithm design, especially if you have implemented any features/ideas worth highlighting.
We have two cases - all of the aggregation expressions are incremental or at least one of them isn't.
If all of them are incremental, then we can just iterate over the data directly.
If at least one of them is incremental, then we will need to handle distincts in a non-trivial way, and we accomplish this using a provided sort method. This does deduplication for us, though we also implement our own check to ensure it.
To handle this case, we write each group to its own tmp_file, and then load a file, sort and buffer read it, and update the aggregation states for each aggr_exprs.
We keep track of the state per expression per group and synthesize it with the final method to get our return.


[Optional] A description of known bugs or limitations, especially if your code did not pass all autograder tests.
N/A

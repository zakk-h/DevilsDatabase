-- baseline plan: 40574 (Estimated IO): MergeJoin(T100k, T10K)
-- example plan: 6519 (Estimated IO): IndexNLJoin (T10K, T100K)
-- Result = 5087

SELECT COUNT(*)
FROM T100K, T10K
WHERE T10K.A = T100K.A
AND T10K.B > 30
AND T10K.A > 3333
AND T100K.C > 0.1;

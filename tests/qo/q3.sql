-- baseline plan: 6974 (Estimated IO): MergeJoin ( IndexNLJoin ( MergeJoin (T10K, T100_1), T1k), T100_2)
-- example plan: 3250 (Estimated IO): BNLJoin ( MergeJoin ( MergeJoin (T100_2, IndexScan (T1k)), T100_1), T10K)
-- Result = 45

CREATE INDEX ON T100K(B);

SELECT COUNT(*)
FROM T10K, T100 AS T100_1, T1k, T100 AS T100_2
WHERE T10K.B = T100_1.A
AND T100_1.B = T100_2.B
AND T100_1.A = T1k.A
AND T10K.A > 10
AND T1k.A = 99;

-- baseline plan: 48961 (Estimated IO): IndexNLJoin ( IndexNLJoin ( MergeJoin ( IndexNLJoin (MergeJoin (T10K, T100_1) T1k_1), T100_2), T1k_2), T100K)
-- example plan: 6740 (Estimated IO): IndexNLJoin ( MergeJoin (T10K, MergeJoin ( IndexNLJoin (IndexNLJoin (T100_1, T1k_1), T100K), T100_2)), T1k_2)
-- Result = 679

CREATE INDEX ON T100K(B);

SELECT COUNT(*)
FROM T10K, T100 AS T100_1, T1k AS T1k_1, T100 AS T100_2, T1k AS T1k_2, T100K
WHERE T10K.B = T100_1.A
AND T1K_1.A = T100K.A
AND T100_1.B = T100_2.B
AND T10K.A = T1k_2.A
AND T100_1.A = T1k_1.A
AND T100K.B > 11;

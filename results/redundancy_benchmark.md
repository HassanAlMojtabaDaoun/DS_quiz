Policies: replication N=3 | erasure coding k=4, m=2

| workload | scheme | files | logical MiB | raw MiB on disk | overhead | upload MiB/s | files/s | repair read MiB | repair written MiB | repair traffic MiB | read per rebuilt byte |
|---|---|---|---|---|---|---|---|---|---|---|---|
| small | replication | 5000 | 125.97 | 339.47 | 3.0 | 0.49 | 19.5 | 56.61 | 56.61 | 113.23 | 1.0 |
| small | ec | 5000 | 125.97 | 169.74 | 1.5 | 0.47 | 18.6 | 113.16 | 33.25 | 146.42 | 3.4 |
| large | replication | 10 | 1758.12 | 5082.36 | 3.0 | 85.7 | 0.5 | 677.0 | 677.0 | 1354.0 | 1.0 |
| large | ec | 10 | 1758.12 | 2541.18 | 1.5 | 86.91 | 0.5 | 1512.12 | 454.78 | 1966.9 | 3.32 |

Theory: replication overhead = N, repair reads 1 byte per byte rebuilt; EC overhead = (k+m)/k = 1.50, repair reads k = 4 bytes per byte rebuilt.

small: node4 failure detected in 15.2 s, repaired in 156.1 s, degraded reads: replication 20/20 OK, ec 20/20 OK

large: node4 failure detected in 15.3 s, repaired in 52.2 s, degraded reads: replication 10/10 OK, ec 10/10 OK

# Raft

Maybe not ideal/perfect but seems mostly working for 2B ...

```
RAFT_LOG=none # none | debug | trace

python3 dstest.py \
>    TestInitialElection2A TestReElection2A  TestManyElections2A \
>    TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B \
>    TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B \
>    -p 10 -n 20 --race
Running with the race detector

┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                   ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestInitialElection2A  │      0 │    20 │  3.58 ± 0.27 │
│ TestReElection2A       │      0 │    20 │  5.27 ± 0.50 │
│ TestManyElections2A    │      0 │    20 │  6.15 ± 0.30 │
│ TestBasicAgree2B       │      0 │    20 │  1.60 ± 0.35 │
│ TestRPCBytes2B         │      0 │    20 │  3.38 ± 0.43 │
│ TestFollowerFailure2B  │      0 │    20 │  5.53 ± 0.44 │
│ TestLeaderFailure2B    │      0 │    20 │  5.97 ± 0.38 │
│ TestFailAgree2B        │      0 │    20 │  5.26 ± 0.90 │
│ TestFailNoAgree2B      │      0 │    20 │  4.47 ± 0.42 │
│ TestConcurrentStarts2B │      0 │    20 │  1.84 ± 0.46 │
│ TestRejoin2B           │      0 │    20 │  6.22 ± 1.08 │
│ TestBackup2B           │      0 │    20 │ 32.73 ± 1.00 │
│ TestCount2B            │      0 │    20 │  3.74 ± 0.14 │
└────────────────────────┴────────┴───────┴──────────────┘
```
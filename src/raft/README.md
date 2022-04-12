# Raft

Maybe not ideal/perfect but seems mostly working for 2C ...

```
# For turning on or off logs, set `RAFT_LOG=none` # none | debug | trace
RAFT_LOG=debug python3 dstest.py     TestInitialElection2A TestReElection2A  TestManyElections2A     TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B     TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B     TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C     TestReliableChurn2C TestUnreliableChurn2C     -p 4 -n 50 --race
Running with the race detector

┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                    ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestInitialElection2A   │      0 │    50 │  3.58 ± 0.20 │
│ TestReElection2A        │      0 │    50 │  5.65 ± 0.55 │
│ TestManyElections2A     │      0 │    50 │  7.14 ± 0.87 │
│ TestBasicAgree2B        │      0 │    50 │  1.53 ± 0.17 │
│ TestRPCBytes2B          │      0 │    50 │  3.30 ± 0.12 │
│ TestFollowerFailure2B   │      0 │    50 │  5.55 ± 0.13 │
│ TestLeaderFailure2B     │      0 │    50 │  5.97 ± 0.49 │
│ TestFailAgree2B         │      0 │    50 │  6.28 ± 0.39 │
│ TestFailNoAgree2B       │      0 │    50 │  6.16 ± 0.50 │
│ TestConcurrentStarts2B  │      0 │    50 │  1.27 ± 0.14 │
│ TestRejoin2B            │      0 │    50 │  6.38 ± 1.23 │
│ TestBackup2B            │      0 │    50 │ 31.16 ± 1.36 │
│ TestCount2B             │      0 │    50 │  2.74 ± 0.09 │
│ TestPersist12C          │      0 │    50 │  4.99 ± 0.39 │
│ TestPersist22C          │      0 │    50 │ 29.90 ± 1.02 │
│ TestPersist32C          │      0 │    50 │  3.04 ± 0.72 │
│ TestFigure82C           │      0 │    50 │ 32.63 ± 2.86 │
│ TestUnreliableAgree2C   │      0 │    50 │  7.82 ± 1.23 │
│ TestFigure8Unreliable2C │      0 │    50 │ 46.07 ± 3.96 │
│ TestReliableChurn2C     │      0 │    50 │ 17.31 ± 0.57 │
│ TestUnreliableChurn2C   │      0 │    50 │ 17.08 ± 0.39 │
└─────────────────────────┴────────┴───────┴──────────────┘┘
```
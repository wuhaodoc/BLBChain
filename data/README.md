# Input data

The package does not redistribute the original trace. Supply your authorized trace as `transactions.csv` with a header and five columns:

```csv
index,unused,sender,recipient,value
0,,0000000000000000000000000000000000000001,0000000000000000000000000000000000000002,1
```

The normalizer preserves order, removes optional 0x prefixes, validates addresses and nonnegative integer values, and fails if fewer than the requested number of rows exist. Record source, preprocessing, hash and redistribution terms for your evaluation trace. Synthetic smoke data is generated on demand and is not an evaluation dataset.

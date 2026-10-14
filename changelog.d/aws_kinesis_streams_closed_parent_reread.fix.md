The `aws_kinesis_streams` source no longer re-reads a closed parent shard after a second split. Once a parent's `SHARD_END` lease is deleted, a later deletion of an intermediate child's lease used to make that parent look unleased, so Vector opened it again with `TRIM_HORIZON` and duplicated its records. A missing closed-shard row now stays finished when its descendant lineages already have checkpoints.

authors: romkimchi

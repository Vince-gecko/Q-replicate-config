# Q-replicate-config
Python script to replicate SMB/NFS/Quotas config from Qumulo to Qumulo, based on replication policies

This script allows you to get the following elements on a source Qumulo cluster and replicate it to a target Qumulo cluster :
 - SMB Shares
 - NFS Exports
 - Quotas

These elements are only retrieved when they are defined on a path that is replicated on the target cluster
If the target path differs from the source, script will automatically translate it
If elements exists on the target cluster, they are updated, if not they are created

At this point, there is no mirroring capabilities, so if you delete a share on the source cluster, the script won't delete it on the target

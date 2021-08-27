# Q-replicate-config
Python script to replicate SMB/NFS/Quotas config from Qumulo to Qumulo, based on replication policies

This script allows you to get the following elements on a source Qumulo cluster and replicate it to a target Qumulo cluster :
 - SMB Shares
 - NFS Exports
 - Quotas

Credentials are stored in the credentials.json file. Be careful password is stored in plaintext so you should create a dedicated user (this will be documented later)

To start replication :
python3 ./q-replicate-config.py

Logs are stored in q-replicate-config.log

These elements are only retrieved when they are defined on a path that is replicated on the target cluster
If the target path differs from the source, script will automatically translate it
If elements exists on the target cluster, they are updated, if not they are created

At this point, there is no mirroring capabilities, so if you delete a share on the source cluster, the script won't delete it on the target

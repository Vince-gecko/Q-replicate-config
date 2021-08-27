# Q-replicate-config
Python script to replicate SMB/NFS/Quotas config from Qumulo to Qumulo, based on replication policies

# Requirements :
 - python3
 - qumulo_api (pip install qumulo_api)

# What does this script does ? :
This script allows you to get the following elements on a source Qumulo cluster and replicate it to a target Qumulo cluster, based on replications policies :
 - SMB Shares
 - NFS Exports
 - Quotas

# How does it works :

Script can be installed from any computer that must be able to access source and target cluster.

Credentials are stored in the credentials.json file. Be careful password is stored in plaintext so you should create a dedicated user (this will be documented later)

Logs are stored in q-replicate-config.log

Scripts starts by analyzing replication configured between the source and the target cluster defined in the credentials.json file (other replications are ignored).
Once it gets the source path it :
 - Creates a translation table to match source path and target path if they differs
 - Gets Quotas / SMB shares / NFS exports defined under each replicated path
 - Updates or creates Quotas / SMB Shares / NFS exports on tbe target cluster

For now, there is no mirroring option, so if you delete an element on the source (Quota, Share, Export), it won't be deleted on the target

To start replication :
python3 ./q-replicate-config.py

__Author__ = "Vincent Lamy"
__version__ = "2021.0830"

from q_functions import *

# Logging Details
logging.basicConfig(filename='q-replicate-config.log', level=logging.INFO,
                    format='%(asctime)s,%(levelname)s,%(message)s')


# Read credentials
json_file = open('./credentials.json', 'r')
json_data = json_file.read()
json_object = json.loads(json_data)
json_file.close()

# Parse primary cluster credentials
primary_cluster_address = json_object['primary_cluster_address']
primary_port_number = json_object['primary_port_number']
primary_username = json_object['primary_username']
primary_password = json_object['primary_password']

# Parse secondary cluster credentials
secondary_cluster_address = json_object['secondary_cluster_address']
secondary_port_number = json_object['secondary_port_number']
secondary_username = json_object['secondary_username']
secondary_password = json_object['secondary_password']

# Connect to the primary cluster
try:
    prc = RestClient(primary_cluster_address, primary_port_number)
    prc.login(primary_username, primary_password)
    logging.info('main,  Connection established with {}'.format(primary_cluster_address))
except Exception as err:
    logging.info('main,  Connection cannot be established with {}'.format(primary_cluster_address))
    logging.info(
        'Error message is {}'.format(err.__dict__))
    logging.info('main,  Ending program now')
    quit()

# Connect to the secondary cluster
try:
    src = RestClient(secondary_cluster_address, secondary_port_number)
    src.login(secondary_username, secondary_password)
    logging.info('main,  Connection established with {}'.format(secondary_cluster_address))
except Exception as err:
    logging.info('main,  Connection cannot be established with {}'.format(secondary_cluster_address))
    logging.info(
        'Error message is {}'.format(err.__dict__))
    logging.info('main,  Ending program now')
    quit()

# Get All Replication configured on primary cluster
all_repl = prc.replication.list_source_relationship_statuses()
tgt_cluster_id = get_cluster_id(src, logging)

# Extract only replication where target is secondary cluster
path_lst = []
path_translation = {}
for repl in all_repl:
    test_repl = is_ip_on_cluster(tgt_cluster_id, repl['target_address'], secondary_port_number,
                                 secondary_username, secondary_password, logging)
    if test_repl:
        logging.info(
            'main,  Replication id {} has target ip {} and will be processed'.format(repl['id'],
                                                                                     repl['target_address']))
        # Retrieve source path

        path_lst.append(repl['source_root_path'])
        path_translation[repl['source_root_path']] = repl['target_root_path']
        logging.info(
            'main, Source path to be processed is {} and its target path is {}'.format(repl['source_root_path'],
                                                                                       repl['target_root_path']))
    else:
        logging.info(
            'main,  Replication id {} has target ip {} and won\'t processed'.format(repl['id'], repl['target_address']))

# Retrieve SMB shares, NFS exports and quotas related to each source path in path_lst
# Generates 3 json files for SMB, NFS and Quotas
for path in path_lst:
    # Filenames pattern will refer the actual path
    smb_file = './smb' + path.rstrip(path[-1]).replace('/', '-') + '.json'
    nfs_file = './nfs' + path.rstrip(path[-1]).replace('/', '-') + '.json'
    quotas_file = './quotas' + path.rstrip(path[-1]).replace('/', '-') + '.json'
    # If files already exists, remove them and log a message
    if os.path.exists(smb_file):
        os.remove(smb_file)
        logging.info(
            'main,  File {} already exists --> we removed it'.format(smb_file))
    if os.path.exists(nfs_file):
        os.remove(nfs_file)
        logging.info(
            'main,  File {} already exists --> we removed it'.format(nfs_file))
    if os.path.exists(quotas_file):
        os.remove(quotas_file)
        logging.info(
            'main,  File {} already exists --> we removed it'.format(quotas_file))

    # Get SMB Shares related to this path
    smb_shares = get_smb_shr(prc, src, logging, path)
    f = open(smb_file, "w")
    f.write(smb_shares)
    f.close()

    # Get NFS exports related to this path
    nfs_exports = get_nfs_exp(prc, logging, path)
    f = open(nfs_file, "w")
    f.write(nfs_exports)
    f.close()

    # Get Quotas to replicate
    quotas = get_quotas(prc, logging, path)
    f = open(quotas_file, "w")
    f.write(quotas)
    f.close()

    replicate_quotas(prc, src, logging, quotas_file, path_translation)
    replicate_nfs(src, logging, nfs_file, path_translation)
    replicate_smb(src, logging, smb_file, path_translation)

    # Clean temporary files
    if os.path.exists(smb_file):
        os.remove(smb_file)
        logging.info(
            'main,  Temporary file {} has been removed'.format(smb_file))
    if os.path.exists(nfs_file):
        os.remove(nfs_file)
        logging.info(
            'main,  Temporary file {} has been removed'.format(nfs_file))
    if os.path.exists(quotas_file):
        os.remove(quotas_file)
        logging.info(
            'main,  Temporary file {} has been removed'.format(quotas_file))


# Closing connection to clusters
prc.close()
src.close()
logging.info('main,  Connection ended with {}'.format(primary_cluster_address))
logging.info('main,  Connection ended with {}'.format(secondary_cluster_address))

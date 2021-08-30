__Author__ = "Vincent Lamy"
__version__ = "2021.0830"
import json
import os
import logging
from qumulo.rest_client import RestClient

# Returns the path related to the id
import qumulo.rest.nfs


# retrieve Qumulo cluster ID
def get_cluster_id(rc, logging):
    state = rc.node_state.get_node_state()
    logging.info('get_cluster_id,  Qumulo cluster {} ID is {}'.format(rc.conninfo.host, state['cluster_id']))
    return state['cluster_id']


# check if IP belongs to the cluster
def is_ip_on_cluster(cluster_id, secondary_cluster_address, secondary_port_number, secondary_username,
                     secondary_password, logging):
    # Try to connect to the IP address with secondary cluster credentials
    try:
        rc = RestClient(secondary_cluster_address, secondary_port_number)
        rc.login(secondary_username, secondary_password)
        logging.info('is_ip_on_cluster,  Connection established with {}'.format(secondary_cluster_address))
        logging.info('is_ip_on_cluster,  {} is a Qumulo cluster'.format(secondary_cluster_address))
        tgt_id = get_cluster_id(rc, logging)
        if cluster_id == tgt_id:
            logging.info('is_ip_on_cluster, IP address {} belongs to cluster {}'.
                         format(secondary_cluster_address, tgt_id))
            rc.close()
            return True
        else:
            logging.info('is_ip_on_cluster, IP address {} do not belongs to cluster {}'.
                         format(secondary_cluster_address, tgt_id))
            rc.close()
            return False
    except Exception as err:
        logging.info('is_ip_on_cluster,  Connection cannot be established with {}'.format(secondary_cluster_address))
        logging.info('is_ip_on_cluster,  Credentials for {} seems not correct or it is not a Qumulo cluster'
                     .format(secondary_cluster_address))
        logging.info(
            'Error message is {}'.format(err.__dict__))
        return False


# Returns the path related to the id
def convert_id_to_path(rc, logging, dir_id):
    file_attr = rc.fs.get_file_attr(dir_id)
    logging.info(
        'convert_id_to_path, id {} on cluster {} refers to path {}'.format(dir_id, rc.conninfo.host, file_attr['path']))
    return file_attr['path']


# Returns the id related to the path
def convert_path_to_id(rc, logging, path):
    file_attr = rc.fs.get_file_attr(path)
    logging.info(
        'convert_path_to_id, path {} on cluster {} refers to id {}'.format(path, rc.conninfo.host, file_attr['id']))
    return file_attr['id']


# Get all SMB shares defined under the path in argument
def get_smb_shr(prc, src, logging, path):
    shares = []
    # Retrieve all SMB Shares
    try:
        all_shr = prc.smb.smb_list_shares()
        # extract only shares defined under path
        for shr in all_shr:
            if path.rstrip(path[-1]) in shr['fs_path']:
                logging.info(
                    'get_smb_shr, Share {} will be replicated'.format(shr['share_name']))
                # Check if there is an ACE referencing LOCAL trustee - if so, get its local name and translate
                # for the secondary cluster if username exists on it - discard if it doesn't
                final_perms = []
                for perm in shr['permissions']:
                    if perm['trustee']['domain'] == "LOCAL":

                        # Get username from auth_id
                        src_ident = prc.auth.find_identity(auth_id=perm['trustee']['auth_id'])

                        # Check if username exists on secondary cluster and gets its auth_id and sid
                        try:
                            tgt_ident = src.auth.find_identity(domain='LOCAL',name=src_ident['name'])
                            logging.info(
                                'get_smb_shr, Username {} exists on secondary cluster, '
                                'translating auth_id from {} to {} and sid from {} to {}'.
                                    format(src_ident['name'], src_ident['auth_id'], tgt_ident['auth_id'],
                                           src_ident['sid'], tgt_ident['sid']))
                            perm['trustee']['auth_id'] = tgt_ident['auth_id']
                            perm['trustee']['sid'] = tgt_ident['sid']
                            final_perms.append(perm)
                        except Exception as err:
                            # If username do not exists on destination, remove the ACE
                            logging.info(
                                'get_smb_shr, Username {} do not exists on cluster {}'.
                                    format(src_ident['name'], src.conninfo.host))
                            logging.info(
                                'get_smb_shr, Discarding ACE for user {} on share {}'.
                                    format(src_ident['name'], shr['share_name']))
                    else:
                        final_perms.append(perm)
                shr['permissions'] = final_perms
                shares.append(shr)
        shares = json.dumps(shares, indent=4)
        return shares
    except Exception as err:
        logging.error(
            'get_smb_shr, There was an issue : Can not retrieve SMB Shares from {}'.
            format(prc.conninfo.host))
        logging.error(
            'get_smb_shr, Error message is {}'.format(err.__dict__))
        logging.info('get_smb_shr, Ending program now')
        quit()


# Get all NFS exports defined under the path in argument
def get_nfs_exp(rc, logging, path):
    exports = []
    # Retrieve all NFS Exports
    try:
        all_exp = rc.nfs.nfs_list_exports()
        # extract only exports defined under path
        for exp in all_exp:
            if path.rstrip(path[-1]) in exp['fs_path']:
                exports.append(exp)
                logging.info(
                    'get_nfs_exp, Export {} will be replicated'.format(exp['export_path']))
        exports = json.dumps(exports, indent=4)
        return exports
    except Exception as err:
        logging.error(
            'get_nfs_exp, There was an issue : Can not retrieve NFS exports from {}'.
            format(rc.conninfo.host))
        logging.error(
            'get_nfs_exp, Error message is {}'.format(err.__dict__))
        logging.info('get_nfs_exp, Ending program now')
        quit()


# Get all quotas defined under the path in argument
def get_quotas(rc, logging, path):
    quotas = []
    # Retrieve all quotas
    try:
        all_quotas = rc.quota.get_all_quotas()
        # API sends a list with quotas and paging, we don't use paging item
        for elements in all_quotas:
            # Just get info from quotas, not paging
            if elements['quotas']:
                for quota in elements['quotas']:
                    # Get path from id
                    quota_path = convert_id_to_path(rc, logging, quota['id'])
                    if path.rstrip(path[-1]) in quota_path:
                        quotas.append(quota)
                        logging.info(
                            'get_quotas, Quota for directory {} will be replicated'.format(path))
        quotas = json.dumps(quotas, indent=4)
        return quotas
    except Exception as err:
        logging.error(
            'get_quotas, There was an issue : Can not retrieve quotas from {}'.
            format(rc.conninfo.host))
        logging.error(
            'get_quotas, Error message is {}'.format(err.__dict__))
        logging.info('get_quotas, Ending program now')
        quit()


# Replicate quotas from source to target cluster
def replicate_quotas(prc, src, logging, src_file, path_translation):
    logging.info(
        'replicate_quotas, Start replicating quotas from file {}'.format(src_file))
    # Parse json source quota file
    with open(src_file) as src_json:
        src_data = json.loads(src_json.read())
        for quota in src_data:
            src_path = convert_id_to_path(prc, logging, quota['id'])
            dst_path = src_path
            logging.info(
                'replicate_quotas, Source quota on path {} (id : {}) has the following limit : {} bytes'.
                format(src_path, quota['id'], quota['limit']))
            # Convert source path to destination path and retrieve id from target
            for sdir, ddir in path_translation.items():
                if sdir in src_path:
                    dst_path = dst_path.replace(sdir, ddir)
            dst_id = convert_path_to_id(src, logging, dst_path)
            logging.info(
                'replicate_quotas, Source path {} translated to destination path {} and its id is {}'.
                format(src_path, dst_path, dst_id))
            # Check if quota already exists on target cluster
            try:
                check_quota = src.quota.get_quota_with_status(dst_id)
                logging.info(
                    'replicate_quotas, Quota on path {} already exists on target cluster {} and its limit is {} bytes'.
                    format(dst_path, src.conninfo.host, check_quota['limit']))
                # If source limits differs from target limit, update the quota on target cluster
                if quota['limit'] != check_quota['limit']:
                    logging.info(
                        'replicate_quotas, Quota limit on source path {} differs from limit on target path {} ({} != '
                        '{})'.format(src_path, dst_path, quota['limit'], check_quota['limit']))
                    # Updating quota on target cluster
                    try:
                        response = src.quota.update_quota(dst_id, quota['limit'])
                        logging.info(
                            'replicate_quotas, Quota on target path {} was updated, new limit is {} bytes'.
                            format(dst_path, response['limit']))
                    except Exception as err:
                        logging.error(
                            'replicate_quotas, There was an issue : Quota on target path {} was not updated'.
                            format(dst_path))
                        logging.error(
                            'replicate_quotas, Error message is {}'.format(err.__dict__))
                else:
                    logging.info(
                        'replicate_quotas, Quota limit on source path {} target path {} are the same ({}), leave it '
                        'unchanged'.format(src_path, dst_path, quota['limit']))
            # If quota do not exists, RestClient returns a 404 code, handled as exception
            except Exception as err:
                logging.info(
                    'replicate_quotas : Quota on path {} do not exists on target cluster {}'.
                    format(dst_path, src.conninfo.host))
                # Create quota on the target cluster
                try:
                    response = src.quota.create_quota(dst_id, quota['limit'])
                    logging.info(
                        'replicate_quotas, Quota on target path {} was created, limit is {} bytes'.
                        format(dst_path, response['limit']))
                except Exception as err:
                    logging.error(
                        'replicate_quotas, There was an issue : Quota on target path {} was not created'.
                        format(dst_path))
                    logging.error(
                        'replicate_quotas, Error message is {}'.format(err.__dict__))
    logging.info(
        'replicate_quotas, End replicating quotas from file {}'.format(src_file))


# Replication NFS exports from source to target cluster
def replicate_nfs(src, logging, src_file, path_translation):
    logging.info(
        'replicate_nfs, Start replicating NFS exports from file {}'.format(src_file))
    # Parse json source nfs file
    with open(src_file) as src_json:
        src_data = json.loads(src_json.read())
        for export in src_data:
            # Check if export already exists on target cluster
            try:
                dst_export = src.nfs.nfs_get_export(export['export_path'])
                logging.info(
                    'replicate_nfs, Export path {} already exists on target cluster {}'.
                    format(export['export_path'], src.conninfo.host))
                # If so, translate source export id to target export id and then update the export
                logging.info(
                    'replicate_nfs, Translating export id for export path {} from id {} to id {}'.
                    format(export['export_path'], export['id'], dst_export['id']))
                export['id'] = dst_export['id']
                # Convert source path to destination path
                dst_path = export['fs_path']
                for sdir, ddir in path_translation.items():
                    if sdir in export['fs_path']:
                        dst_path = dst_path.replace(sdir, ddir)
                # update the export on target cluster
                try:
                    # Format restriction
                    restrictions = []
                    for restrict in export['restrictions']:
                        restrictions.append(qumulo.rest.nfs.NFSExportRestriction(restrict))
                    src.nfs.nfs_modify_export(id_=export['id'], export_path=export['export_path'], fs_path=dst_path,
                                              description=export['description'], restrictions=restrictions,
                                              fields_to_present_as_32_bit=export['fields_to_present_as_32_bit'])
                    logging.info(
                        'replicate_nfs, NFS export id {} for path {} has been updated'.
                        format(dst_export['id'], export['fs_path']))
                except Exception as err:
                    logging.error(
                        'replicate_nfs, Cannot update NFS export id {} for path {} '.
                        format(dst_export['id'], export['fs_path']))
                    logging.error(
                        'replicate_nfs, Error message is {}'.format(err.__dict__))

            except Exception as err:
                logging.info(
                    'replicate_nfs, Export path {} do not exists on target cluster {}'.
                    format(export['export_path'], src.conninfo.host))
                # Create the export on target cluster
                try:
                    # Format restriction
                    restrictions = []
                    for restrict in export['restrictions']:
                        restrictions.append(qumulo.rest.nfs.NFSExportRestriction(restrict))
                    # Convert source path to destination path
                    dst_path = export['fs_path']
                    for sdir, ddir in path_translation.items():
                        if sdir in export['fs_path']:
                            dst_path = dst_path.replace(sdir, ddir)
                    src.nfs.nfs_add_export(export_path=export['export_path'],
                                           fs_path=dst_path,
                                           description=export['description'], restrictions=restrictions,
                                           fields_to_present_as_32_bit=export['fields_to_present_as_32_bit'])
                    logging.info(
                        'replicate_nfs, NFS export for path {} has been created'.
                        format(export['fs_path']))
                except Exception as err:
                    logging.error(
                        'replicate_nfs, Cannot create NFS export for path {}'.
                        format(export['fs_path']))
                    logging.error(
                        'replicate_nfs, Error message is {}'.format(err.__dict__))


# Replication SMB shares from source to target cluster
def replicate_smb(src, logging, src_file, path_translation):
    logging.info(
        'replicate_smb, Start replicating SMB Shares from file {}'.format(src_file))
    # Parse json source SMB file
    with open(src_file) as src_json:
        src_data = json.loads(src_json.read())
        for share in src_data:
            # Check if share already exists on target cluster
            try:
                dst_share = src.smb.smb_list_share(share['share_name'])
                logging.info(
                    'replicate_smb, Share {} already exists on target cluster {}'.
                    format(share['share_name'], src.conninfo.host))
                # Convert source path to destination path (removing trailing / cause API sends fs_path without it)
                dst_path = share['fs_path']
                for sdir, ddir in path_translation.items():
                    if sdir.rstrip('/') in share['fs_path']:
                        dst_path = dst_path.replace(sdir.rstrip('/'), ddir.rstrip('/'))
                try:
                    src.smb.smb_modify_share(old_name=share['share_name'],
                                             fs_path=dst_path,
                                             description=share['description'],
                                             allow_fs_path_create=False,
                                             access_based_enumeration_enabled=share['access_based_enumeration_enabled'],
                                             default_file_create_mode=share['default_file_create_mode'],
                                             default_directory_create_mode=share['default_directory_create_mode'],
                                             permissions=share['permissions'],
                                             require_encryption=share['require_encryption'],
                                             network_permissions=share['network_permissions'])
                    logging.info(
                        'replicate_smb, SMB Share {} for path {} has been updated'.
                        format(share['share_name'], dst_path))
                except Exception as err:
                    print(err)
                    logging.error(
                        'replicate_smb, Cannot update SMB Share {} for path {} '.
                        format(share['share_name'], dst_path))
                    logging.error(
                        'replicate_smb, Error message is {}'.format(err.__dict__))

            except Exception as err:
                logging.info(
                    'replicate_smb, Share {} do not exists on target cluster {}'.
                    format(share['share_name'], src.conninfo.host))
                # Create the share on target cluster
                try:
                    # Convert source path to destination path (removing trailing / cause API sends fs_path without it)
                    dst_path = share['fs_path']
                    for sdir, ddir in path_translation.items():
                        if sdir.rstrip('/') in share['fs_path']:
                            dst_path = dst_path.replace(sdir.rstrip('/'), ddir.rstrip('/'))
                    src.smb.smb_add_share(share_name=share['share_name'],
                                          fs_path=dst_path,
                                          description=share['description'],
                                          read_only=None,
                                          allow_guest_access=None,
                                          allow_fs_path_create=False,
                                          access_based_enumeration_enabled=share['access_based_enumeration_enabled'],
                                          default_file_create_mode=share['default_file_create_mode'],
                                          default_directory_create_mode=share['default_directory_create_mode'],
                                          permissions=share['permissions'],
                                          require_encryption=share['require_encryption'],
                                          network_permissions=share['network_permissions'])
                    logging.info(
                        'replicate_smb, SMB Share {} for path {} has been created'.
                        format(share['share_name'], share['fs_path']))
                except Exception as err:
                    logging.error(
                        'replicate_smb, Cannot create SMB Share {} for path {}'.
                        format(share['share_name'], dst_path))
                    logging.error(
                        'replicate_smb, Error message is {}'.format(err.__dict__))

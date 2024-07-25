/* Copyright (C) 2020 NooBaa */
'use strict';

const dbg = require('../util/debug_module')(__filename);
const _ = require('lodash');
const path = require('path');
const minimist = require('minimist');
const config = require('../../config');
const P = require('../util/promise');
const nb_native = require('../util/nb_native');
const cloud_utils = require('../util/cloud_utils');
const native_fs_utils = require('../util/native_fs_utils');
const mongo_utils = require('../util/mongo_utils');
const SensitiveString = require('../util/sensitive_string');
const ManageCLIError = require('../manage_nsfs/manage_nsfs_cli_errors').ManageCLIError;
const ManageCLIResponse = require('../manage_nsfs/manage_nsfs_cli_responses').ManageCLIResponse;
const manage_nsfs_glacier = require('../manage_nsfs/manage_nsfs_glacier');
const manage_nsfs_logging = require('../manage_nsfs/manage_nsfs_logging');
const noobaa_cli_diagnose = require('../manage_nsfs/diagnose');
const nsfs_schema_utils = require('../manage_nsfs/nsfs_schema_utils');
const { CONFIG_SUBDIRS } = require('./manage_nsfs_constants');
const nc_mkm = require('../manage_nsfs/nc_master_key_manager').get_instance();

class Conf_Dir_Access {

    /**
     * @param {string} config_root configuration directory path
     * 
     */
    constructor(config_root, config_root_backend, fs_root) {
        this.config_root = config_root;
        this.accounts_dir = path.join(config_root, CONFIG_SUBDIRS.ACCOUNTS);
        this.root_accounts_dir = path.join(config_root, CONFIG_SUBDIRS.ROOT_ACCOUNTS);
        this.access_keys_dir = path.join(config_root, CONFIG_SUBDIRS.ACCESS_KEYS);
        this.buckets_dir = path.join(config_root, CONFIG_SUBDIRS.BUCKETS);
        this.fs_context = native_fs_utils.get_process_fs_context(config_root_backend);
    }

    _json(filename){
        return filename + ".json"
    }

    _symlink(filename){
        return filename + ".symlink"
    }

    /**
     * get_config_data will read a config file and return its content 
     * while omitting secrets if show_secrets flag was not provided
     * @param {string} config_file_path
     * @param {boolean} [show_secrets]
     */
    async _get_config_data(config_file_path, show_secrets = false, decrypt_secret_key = false) {
        const { data } = await nb_native().fs.readFile(this.fs_context, config_file_path);
        const config_data = _.omit(JSON.parse(data.toString()), show_secrets ? [] : ['access_keys']);
        if (decrypt_secret_key) config_data.access_keys = await nc_mkm.decrypt_access_keys(data);
        return config_data;
    }

    async account_by_name_path(root_name, iam_name){
        if(!iam_name) iam_name = root_name;
    
        const old_path = path.join(this.accounts_dir, this._json(root_name));
        const new_path = path.join(this.root_accounts_dir, root_name, this._symlink(iam_name));

        const new_exists = await native_fs_utils.is_path_exists(this.fs_context, new_path);
        const old_exists = await native_fs_utils.is_path_exists(this.fs_context, old_path);
        if (new_exists) return new_path;
        if (old_exists) return old_path;
        return new_path;
    }

    async _account_path(account){
    
        const old_path = path.join(this.accounts_dir, this._json(account._id));
        const new_path = path.join(this.accounts_dir, this._json(account.name));

        const new_exists = await native_fs_utils.is_path_exists(this.fs_context, new_path);
        return new_exists ? new_path : old_path;
    }

    account_by_access_key_path(access_key){
        return path.join(this.access_keys_dir, this._symlink(access_key));
    }

    async account_by_name(root_name, iam_name){
        const account_path = await this.account_by_name_path(root_name, iam_name);
        const account = await this._get_config_data(account_path);
        return account;
    }

    async account_by_access_key(access_key, decerypt_secretes){
        const account_path = this.account_by_access_key_path(access_key);
        const account = await this._get_config_data(account_path, decerypt_secretes, decerypt_secretes);
        return account;
    }

    async file_by_json(dir, filename, check_existance){
        const file_path = path.join(dir, this._json(filename));
        if (check_existance && !await this.file_exists(file_path)) {
            //file does not exist, ignore.
            return;
        }
        return await this._get_config_data(file_path);
    }

    account_by_id_path(id){
        return path.join(this.accounts_dir, this._json(id));
    }

    async account_by_id(id, check_existance) {
        return await this.file_by_json(this.accounts_dir, id, check_existance);
    }

    async update_account(account_str, removed_access_key){
        //account_str is a stringified object, so it's sensitive strings were
        //already unwrapped
        const account = JSON.parse(account_str);
        nsfs_schema_utils.validate_account_schema(account);
        const account_path = await this._account_path(account);
        await native_fs_utils.update_config_file(this.fs_context, this.accounts_dir,
            account_path, JSON.stringify(account));

        if (removed_access_key) {
            const access_key_symlink = this.account_by_access_key_path(removed_access_key);
            await nb_native().fs.unlink(this.fs_context, access_key_symlink);
        } else {
            //create new access key symlinks, if necessary
            for(const access_key in account.access_keys){
                const access_key_symlink = this.account_by_access_key_path();
                if (await this.file_exists(access_key_symlink, true)) {
                    continue;
                }
                //access key is new, create its symlink
                const account_config_relative_path = path.join('..', this.accounts_dir, this._json(account._id));
                await nb_native().fs.symlink(this.fs_context, account_config_relative_path, access_key_symlink);
            }
        }
    }

    async update_account_username(old_username, new_username, root_name) {
        const old_account_path = await this.account_by_name(root_name, old_username);
        const new_account_path = await this.account_by_name(root_name, new_username);
        await nb_native().fs.rename(this.fs_context, old_account_path, new_account_path);
        /*await this._check_username_already_exists(action, new_username, root_account_name);
        const root_account_old_name = get_symlink_config_file_path(this.root_accounts_dir, old_username, root_account_name);
        await nb_native().fs.unlink(this.fs_context, root_account_old_name);
        await this._symlink_to_account(user_id, this.root_accounts_dir, new_username, root_account_name);*/
    }

    async delete_account(root_account, account_to_delete){
        //delete the json file (either old or new)
        await native_fs_utils.delete_config_file(this.fs_context, this.accounts_dir,
            await this._account_path(account_to_delete));

        //delete the new by-name symlink, if it exists
        const by_name_new = path.join(this.root_accounts_dir, root_account.anem, this._symlink(account_to_delete.name));
        if (await this.file_exists(by_name_new, true /*use_lstat*/)) {
            await nb_native().fs.unlink(this.fs_context, by_name_new);
        }
        
        //if deleting a root account, delete the new root directory
        if (root_account._id === account_to_delete._id) {
            //delete directory of root account (it's empty now)
            await native_fs_utils.folder_delete(path.join(this.root_accounts_dir, root_account.name), this.fs_context);
        }
    }

    async file_exists(file, use_lstat){
        return await native_fs_utils.is_path_exists(this.fs_context, file, use_lstat /*use_lstat*/);
    }

    async list_accounts(){
        return await nb_native().fs.readdir(this.fs_context, this.accounts_dir);
    }

    async list_buckets(){
        return await nb_native().fs.readdir(this.fs_context, this.buckets_dir);
    }

    async bucket_by_name(name, check_existance){
        return await this.file_by_json(this.buckets_dir, name, check_existance);
    }

    async create_account(account, root_name){
        const account_str = JSON.stringify(account);
        nsfs_schema_utils.validate_account_schema(JSON.parse(account_str));
        const account_config_path = path.join(this.accounts_dir, this._json(account._id));
        await native_fs_utils.create_config_file(this.fs_context, this.accounts_dir,
            account_config_path, account_str);
        //for account manager, create a new root user. Otherwise, root is the requesting account. 
        //const root_account_dir = requesting_account.iam_operate_on_root_account ? params.username : requesting_account.name.unwrap();
        //make sure root directory exists
        await native_fs_utils._create_path(path.join(this.root_accounts_dir, root_name), this.fs_context);
        //symlink from by-name account to by-id account json
        const root_account_symlink = await this.account_by_name_path(root_name, account.name);
        await nb_native().fs.symlink(this.fs_context,
            path.join("..", "..", CONFIG_SUBDIRS.ACCOUNTS, this._json(account._id)),
            root_account_symlink
        );
    }
}

// EXPORTS
exports.Conf_Dir_Access = Conf_Dir_Access;

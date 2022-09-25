# EsgynDB Python Installer

## Prerequisite:

- CDH/HDP is installed on Trafodion nodes with web UI enabled, or Apache Hadoop, HBase is installed on the same directory on all nodes
- /etc/hosts contains hostname info for all Trafodion nodes on **installer node**
- support python version 2.6/2.7
- Trafodion server package file is stored on **installer node**
- Passwordless SSH login, one of these two options is needed:
 - Set SSH key pairs against **installer node** and Trafodion nodes
 - Install `sshpass` tool on **installer node**, then input the SSH password during installation using `--enable-pwd` option

> **installer node** can be any nodes as long as it can ssh to Trafodion nodes, it also can be one of the Trafodion nodes

## How to use:
- Two ways:
 - Simply invoke `./db_install.py` to start the installation in guided mode
 - Copy `configs/db_config_default.ini` file to `your_db_config` and modify it, then invoke `./db_config.py --config-file your_db_config` to start installation in config mode
- For a quick install with default settings, you only need to put Trafodion package file in installer's directory, provide CDH/HDP web URL in `your_db_config` file and then it's ready to go!
- Use `./db_install.py --help` for more options
- Invoke `./inspector.py` to get the system basic info on all nodes

# Cluster Pre-configuration Tool

This tool sets up the cluster configurations before installing Hadoop and EsgynDB, which helps you complete the following tasks:

- Set up passwordless SSH.
- Configure local yum repositories.
- Modify `/etc/hosts` and hostnames on all nodes, ntp, selinux, iptables and so on.

## Prerequisite
- Add the hostname and the IP address for all nodes on which EsgynDB installed to `/etc/hosts`.
- Prepare all dependent packages on the image for RedHat.

## How to Use
Run `pre_install.py` and provide the following information.

```
$ ./pre_install.py
**************************************
  Trafodion Pre-Installation ToolKit
**************************************
Enter user name to set up passwordless SSH for [root]: 
Enter remote host SSH Password: 
Confirm Enter remote host SSH Password: 
Enter list of Cloudera Nodes separated by comma, support simple numeric Regular Expression,
 i.e. "n[01-12],n[21-25]","n0[1-5].com": node-[1-2].example.com
Enter full path to iso file:
Enter the gateway of the LAN(used for NTP setting) [192.168.0.0]: 
Enter the subnet mask of the LAN(used for NTP setting) [255.255.255.0]: 
```

# CDH Installation Tool

This tool helps you complete the following tasks:

- Install Mysql and Cloudera distribution (CDH).
- Configure Mysql HA.
- Configure Mysql as the external database for Cloudera distribution.

## Prerequisite
- Prepare a local repository folder (with repodata) for Cloudera Manager.
- Prepare a local repository folder (with repodata) for Cloudera Manager dependencies.
- Prepare a local parcel folder for CDH.
- Prepare a Mysql binary installation package.
- Prepare a Mysql JDBC driver.

## How to Use
1. Configure `configs/cdh_config.ini`.
    - Add the path of the local Mysql binary installation package to `mysql_path`.
    - Add the path of the local Mysql JDBC driver to `mysql_jdbc_path`.
    - Add two hostnames of Mysql to `mysql_hosts`.
    - Add the path (separated by comma) of Cloudera Manager and Cloudera Manager dependencies  to `repo_dir`.
    - Add the path of local parcel folder to `parcel_dir`.
    - Add the hostname (regular expression is valid) of Cloudera agent to `hosts`. The Cloudera Manager will be installed on the node who is running this tool.

2. Run `cdh_install.py` to install Mysql and Cloudera Manager, and configure Mysql HA.

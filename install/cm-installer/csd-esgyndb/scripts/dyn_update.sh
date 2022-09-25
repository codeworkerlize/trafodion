#!/bin/sh

echo "control script: $0"
echo "========================="


source ~/.bashrc

set -x

########################################################
# install license
echo "Esgyn License status:"
licensePath="/etc/trafodion/esgyndb_license"
if [[ -z $EDB_LIC_KEY ]]
then
  rm -f $licensePath
  LicMsg="No license key -- a short grace period allowed to acquire a license.
    Contact Esgyn and send ID file: ${COORDINATING_HOST}:/etc/trafodion/esgyndb_id"
  echo "$LicMsg"
else
  LicMsg=""
  echo -n "$EDB_LIC_KEY" > $licensePath
  decoder -a -f $licensePath

  license_type=$(decoder -t -f $licensePath)
  # check license
  if [[ $license_type == "INTERNAL" ]]
  then
    echo "Internal license -- skipping node check"
  else
    licensed_nodes=$(decoder -n -f $licensePath)
    config_nodes="$(cut -d: -f1 $CONF_DIR/traf_node.properties | sort -u | wc -l)"
    if [[ ! $licensed_nodes =~ ^[0-9]+$ ]] || (( $config_nodes > $licensed_nodes ))
    then
      echo "Error: License is not valid for $config_nodes nodes"
      echo "       Contact Esgyn and send cluster ID file: /etc/trafodion/esgyndb_id"
      exit 1
    fi
  fi
  expireDay=$(decoder -e -f $licensePath)
  currentDay=$(($(date --utc +%s)/86400))
  if [[ ! $expireDay =~ ^[0-9]+$ ]] || (( $expireDay < $currentDay ))
  then
    echo "Error: License has expired"
    echo "       Contact Esgyn and send ID file: ${COORDINATING_HOST}:/etc/trafodion/esgyndb_id"
    exit 1
  fi
fi
echo "========================="

########################################################


# LDAP config
if [[ $TRAFODION_ENABLE_AUTHENTICATION == "YES" ]]
then
  ldap_host_list=""
  for lhost in $ldap_hosts
  do
    ldap_host_list+="  LdapHostname: $lhost\n"
  done
  IFS=";"
  ldap_uid_list=""
  for lid in $ldap_identifiers
  do
    ldap_uid_list+="  UniqueIdentifier: $lid\n"
  done
  unset IFS
  echo "Installing LDAP config: $TRAF_CONF/.traf_authentication_config"
  sed -e "s#{{ldap_certpath}}#$ldap_certpath#" \
      -e "s#{{ldap_domain}}#$ldap_domain#" \
      -e "s#{{ldap_hosts}}#$ldap_host_list#" \
      -e "s#{{ldap_port}}#$ldap_port#" \
      -e "s#{{ldap_identifiers}}#$ldap_uid_list#" \
      -e "s#{{ldap_search_id}}#$ldap_search_id#" \
      -e "s#{{ldap_pwd}}#$ldap_pwd#" \
      -e "s#{{ldap_encrypt}}#$ldap_encrypt#" \
      -e "s#{{ldap_search_group_base}}#$ldap_search_group_base#" \
      -e "s#{{ldap_search_group_object_class}}#$ldap_search_group_object_class#" \
      -e "s#{{ldap_search_group_member_attribute}}#$ldap_search_group_member_attribute#" \
      -e "s#{{ldap_search_group_name_attribute}}#$ldap_search_group_name_attribute#" \
    $CONF_DIR/traf_authentication_config > $TRAF_CONF/.traf_authentication_config

  echo "Checking LDAP configuration"
  ldapconfigcheck || exit 1
else
  echo "LDAP authentication not enabled"
fi

exit 0

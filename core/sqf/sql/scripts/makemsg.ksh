
#
# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@
#
function GetSQcnfg {
# Get SQ Node configuration
 TempList=`grep -o 'node-name=.[^A-Za-z].[0-9]*' $TRAF_CONF/sqconfig | cut -d "=" -f 2 | cut -d ";" -f 1 | sort -u`

 i=0
 for NODE in $TempList
   do
     SQNodeNames[$i]=$NODE
     ((i=i+1))

     done

 # Check that the Node names were corretly added
 NumberOfSQnodes=${#SQNodeNames[*]}
 ExNodeList="$(echo ${SQNodeNames[@]} | tr ' ' ',')"


 if [ ! -z ${ExNodeList[@]} ]; then   
     echo "${ExNodeList[@]}"
 
 else
     echo
     echo "Could not parse $TRAF_CONF/sqconfig file."
     echo "Please ensure sqenv.sh has been sourced and the sqconfig file is valid.  Then, re-run sqgen."
     echo
     exit 1;
 fi

}

#GetSQcnfg
langTypes=("_en" "_cn")
errors_txt=$TRAF_HOME/export/include/sql/SqlciErrors
errors_cat=$TRAF_HOME/export/bin$SQ_MBTYPE/mxcierrors

for langType in ${langTypes[*]}
do
# generate catalog only if it doesn't exist or $error_txt is older
error_txt=$errors_txt$langType".adoc"
error_cat=$errors_cat$langType".cat"
if [ $error_txt -nt $error_cat ]; then
rm -f *ci[Ee]rrors$langType.[ghm]		\
    *ci[Ee]rrors_msg$langType.[ghm]
rm -f $error_cat

# versioning
./msgfileVrsn.ksh	 > SqlciErrors$langType.g

# append error message
cat $error_txt	>> SqlciErrors$langType.g

# reformating
awk -f makemsg.awk SqlciErrors$langType.g > SqlciErrors$langType.m

echo "generating sql message catalog: $error_cat ... "
# generate catalog
gencat $error_cat SqlciErrors$langType.m
fi
done

subs_txt=$TRAF_HOME/export/include/sql/Suberror
subs_cat=$TRAF_HOME/export/bin$SQ_MBTYPE/Suberror
subTypes=("Hbase" "TM")

for langType in ${langTypes[*]}
do
for subType in ${subTypes[*]}
do
# generate catalog only if it doesn't exist or $error_txt is older
sub_txt=$subs_txt$subType$langType".adoc"
sub_cat=$subs_cat$subType$langType".cat"

if [ $sub_txt -nt $sub_cat ]; then
rm -f Suberror$subType$langType.[ghm]
rm -f $sub_cat

# append error message
cat $sub_txt	>> Suberror$subType$langType.g

# reformating
awk -f makemsg.awk Suberror$subType$langType.g > Suberror$subType$langType.m

echo "generating sql message catalog: $sub_cat ... "
# generate catalog
gencat $sub_cat Suberror$subType$langType.m
fi
done
done
#exit 0
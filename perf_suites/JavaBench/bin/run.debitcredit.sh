
OPTION_DEBUG="FALSE"

COMMAND_LINE="$@"

while [[ $# > 0 ]] ; do
key="$1"; shift;
case ${key,,} in
    -d|--debug|debug)				OPTION_DEBUG="TRUE";;
esac
done

if [[ $OPTION_DEBUG = "TRUE" ]] ; then
	run.test.sh benchmark DebitCredit ${COMMAND_LINE}
else
	run.test.sh benchmark DebitCredit measure plans bouncedcs sysinfo ${COMMAND_LINE} 
fi


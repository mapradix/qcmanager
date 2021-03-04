#!/bin/sh

if test -z "$1"; then
   echo "$0 tuc1|tuc2 ip|lp [sample]"
   exit 1
fi
if [ $1 != "tuc1" ] && [ $1 != "tuc2" ] ; then
   echo "$0 tuc1|tuc2 ip|lp [sample]"
   exit 1
fi
if [ $2 != "ip" ] && [ $2 != "lp" ] ; then
   echo "$0 tuc1|tuc2 ip|lp [sample]"
   exit 1
fi

if [ "$1" = "tuc1" ]; then
    TUC=use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague
else
    TUC=use_cases/tuc2_tccm_1518_020m/tuc2_tccm_2015_2018_20m_sumava
fi

QUIET="-q"
#QUIET=""
if test -n "$3"; then
    if [ "$3" != "sample" ]; then
        ./bin/run_manager.py $QUIET -c ${TUC}.yaml,${TUC}_${2}.yaml,$3
    else
        ./bin/run_manager.py $QUIET -c ${TUC}.yaml,${TUC}_${2}.yaml,${TUC}_sample.yaml
    fi
else
    ./bin/run_manager.py $QUIET -c ${TUC}.yaml,${TUC}_${2}.yaml
fi

exit 0

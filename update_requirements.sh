#!/bin/sh

make _virtualenv

REQILE='requirements.txt'
echo ". _virtualenv/bin/activate; REQILE='requirements.txt'; cat ${REQILE} | xargs --max-args=1 --delimiter='\\\n' python3 -m pip install -U; cat ${REQILE} | sed -e 's/[<>=]\+.*//' -e 's/^/^/' -e 's/$/[=]/g' > _${REQILE}; python3 -m pip list --format=freeze | grep -f _${REQILE} > ${REQILE}" | sh

REQILE='requirements-dev.txt'
echo ". _virtualenv/bin/activate; REQILE='requirements-dev.txt'; cat ${REQILE} | xargs --max-args=1 --delimiter='\\\n' python3 -m pip install -U; cat ${REQILE} | sed -e 's/[<>=]\+.*//' -e 's/^/^/' -e 's/$/[=]/g' > _${REQILE}; python3 -m pip list --format=freeze | grep -f _${REQILE} > ${REQILE}" | sh

rm _req*.txt

make clean

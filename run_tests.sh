#!/usr/bin/env bash

RESULT_FILE=tests/results/results.md

echo '```bash' > ${RESULT_FILE}
# echo $0 >> ${RESULT_FILE}
echo 'export PYTHONPATH="$(realpath .):$(realpath ./tests) $(pyenv which python) && cd tests && run_tests.py ; cd ..' >> ${RESULT_FILE}
echo '```' >> ${RESULT_FILE}
echo '' >> ${RESULT_FILE}
echo '' >> ${RESULT_FILE}
echo '# Tests' >> ${RESULT_FILE}

export PYTHONPATH="$(realpath "."):($realpath './tests')" PYTHONUNBUFFERED=1
cd tests/ || exit
$(pyenv which python) run_tests.py | tee --append ../${RESULT_FILE}
cd ..


#!/usr/bin/env bash

RESULT_FILE=tests/results/results.md

echo '```bash' > ${RESULT_FILE}
# echo $0 >> ${RESULT_FILE}
echo 'PYTHONPATH="$(realpath .):$(realpath ./tests) $(pyenv which python) tests/run_tests.py' >> ${RESULT_FILE}
echo '```' >> ${RESULT_FILE}
echo '' >> ${RESULT_FILE}
echo '' >> ${RESULT_FILE}
echo '# Tests' >> ${RESULT_FILE}

PYTHONPATH="$(realpath "."):($realpath './tests')" PYTHONUNBUFFERED=1 $(pyenv which python) tests/run_tests.py >> ${RESULT_FILE}


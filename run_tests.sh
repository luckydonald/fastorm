#!/usr/bin/env bash

RESULT_FILE=tests/results/results.md
mkdir -p tests/results

echo '```bash' > ${RESULT_FILE}
# echo $0 >> ${RESULT_FILE}
echo 'export PYTHONPATH="$(realpath .):$(realpath ./tests) $(pyenv which python) && cd tests && run_tests.py ; cd ..' >> ${RESULT_FILE}
echo '```' >> ${RESULT_FILE}
echo '' >> ${RESULT_FILE}
echo '' >> ${RESULT_FILE}
echo '# Tests' >> ${RESULT_FILE}

export PYTHONPATH="$(realpath "."):($realpath './tests')" PYTHONUNBUFFERED=1
cd tests/ || exit
poetry run python -m unittest discover --pattern="*_tests.py" --start-directory tests --locals

poetry run python run_tests.py | tee -a ../${RESULT_FILE}
cd ..


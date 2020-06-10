del /s/q dist\*
del /s/q build\*
python setup.py sdist
python setup.py bdist_wheel --universal

pip uninstall --yes robotframework-hdfslibrary
python setup.py install
python -m robot.libdoc .\HDFSLibrary doc\HDFSLibrary.html
REM twine upload dist/*
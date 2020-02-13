# PyPi Cheatsheet

How to upload a new package release into PyPi

## Summary

```bash
	git tag -a MAJOR.MINOR.PATCH
	python3 setup.py sdist
	python3 -m twine upload dist/*
	git push --tags origin master
``` 

## Prerequisites

	- Install the latest versions of setuptools, wheel and twine

	`python3 -m pip install --user --upgrade setuptools wheel twine`

	- Needs an account in PyPi and Testing PyPy

	- File  ~/.pypirc file is not needed anymore, although it is useful to remember credentials :-)
	A safer options is to use `keyring` in Linux

	- it is helpful to know if youer package is an [Universal Wheels] package(https://packaging.python.org/guides/distributing-packages-using-setuptools/#universal-wheels). If so, set it up on your `setup.cfg` file.
	
	```
	[bdist_wheel]
	universal=1
	```


## Steps

1. Merge your branch into master
  (you want tags to be point t a commit in master)

	`git checkout master`
	`git merge develop`

2. List your current tags

	`git tag`


3. tag the current MAJOR.MINOR.PATCH release. We use the annotated tags
to upload them to GitHub and mark releases there as well.

Given a version number MAJOR.MINOR.PATCH, increment the:

	1. MAJOR version when you make incompatible API changes,
	2. MINOR version when you add functionality in a backwards-compatible manner, and
	3. PATCH version when you make backwards-compatible bug fixes.
	
	`git tag -a MAJOR.MINOR.PATCH`

	(to delete a tag type `git tag -d <tag>`)

4. Package as source distribution (`sdist`) and binary wheels (`bdist_wheel`). Generated packages are placed into `dists/`.

	`python3 setup.py sdist bdist_wheel`
	
5. Run `twine` to upload all of the archives under `dist/`.

	`python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*`

	You will be prompted for the username and password you registered with Test PyPI.


6. Test that you can install it from the Testing PyPi site

	`python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps <package-name>`

	Since TestPyPI doesn’t have the same packages as the live PyPI, it’s possible that attempting to install dependencies may fail or install something unexpected. While our example package doesn’t have any dependencies, it’s a good practice to avoid installing dependencies when using TestPyPI.

7. Do 5 and 6 with the official PyPi website

	`python3 -m twine upload dist/*` 
	
	You can see if your package has successfully uploaded by navigating to the URL https://pypi.org/project/<package-name>

	`python3 -m pip install --user <package-name>`

# Updating GitHub repo

1. Push master branch and tags to GitHub

	`git push --tags origin master`

# Reviewing the package in PyPi

	Use your credentails in ~/.pipyrc

# Remove named tags 

	- This wil delete the tag in your local repo: `git tag -d 12345`

	- And this will delete it from GitHub: 
	
	`git tag -d 12345`
	`git push origin :refs/tags/12345`

# See also

- [Python Wiki](https://wiki.python.org/moin/TestPyPI)
- [Far McKon website](http://www.farmckon.net/tag/testpypi/)

## Notes for Darshan developers working on documents published on readthedocs.io

### Sign up/log into readthedocs.io

* Visit [ReadtheDocs](https://about.readthedocs.com) to sign up or log into an
  account. You can choose "Sign up with Github" option.
* After logged into the web site, it will leads you to the readthedocs dash
  board, which shows a list of your projects.
* [Read the Docs tutorial](https://docs.readthedocs.com/platform/latest/tutorial/index.html)
  contains detailed information about how to sign up a user count and set up
  the connection to Darshan's github repo.

### Create Darshan project on Read the Docs

* Click "Add project"
* In field "Repository name", enter the Darshan github repo name,
  "darshan-hpc/darshan", and then click "Continue".
* The default settings of Name, Repository URL, Default branch, and Language
  will be pre-filled. Customize them if necessary. Then click "Next".
* As configure file `.readthedocs.yml` is required in the root folder of github
  repo, click "This file exists" to let it add a default file.
* This will lead you to the readthedocs dashboard and the very first build
  should be showing triggered and in progress.
* Note that this will also add a webhook to the Darshan github repo. See it
  from Darshan github repo's Settings, and then Webhooks.

### Change settings of Darshan project on readthedocs Dashboard

* Visit your dashboard at https://app.readthedocs.org/dashboard/
* Select Darshan project
* Click "Settings" on right.
  + At the bottom of this page, select "Build pull requests for this project"
    and click "Save". This will enable rebuild Darshan documents for all pull
    requests.
  + Add a new project maintainer
    * Click "Maintainers" on left
    * Click "Add maintainer" button.
  + Enable Analytics
    * Click "Addons" on left
    * Click "Analytics" tab, select "Analytics enabled" button, and "Save".
  + Environment variables
    * Environment variables set here are for readthedocs to use, for example
      `DARSHAN_INSTALL_PREFIX` is set to the location of installation location
      of Darshan.
    * Change existing variable must be done by first deleting it and add a new
      one.

### Configuration files

* File `conf.py` must be stored in the root folder of Darshan's repo.
* File `.readthedocs.yaml` must be stored in the root folder of Darshan's repo.
* Darshan's documents require `darshan-util` to be built and installed first,
  before installing pydarshan. See the settings of `pre_install` in file
  `.readthedocs.yaml`.
* Building of `darshan-runtime` is not required.
* Mater file is `index.rst` must be stored in the root folder.
* File `index.rst` includes the following documents.
  + darshan-runtime/doc/darshan-runtime
  + darshan-util/doc/darshan-util
  + docs/darshan-modularization.rst
  + darshan-util/pydarshan/docs/readme
  + darshan-util/pydarshan/docs/install
  + darshan-util/pydarshan/docs/usage
  + darshan-util/pydarshan/docs/api/pydarshan/modules

### Previewing updates

Use the following steps to review documentation changes before they are
merged to the main branch:
* Modify the appropriate files (see `index.rst` and the list of included
  documents described in the preceding subsection)
* Open a pull request on GitHub with your changes
* Wait a few minutes for automated actions to occur on ReadTheDocs
* Log into https://app.readthedocs.org
* Navigate to the Darshan dashboard and click the `Builds` tab
* Select the corresponding pull request and click the `View Docs` button
* Validate the changes, push additional commits if needed, and merge the PR
  when ready
* Wait a few minutes for ReadTheDocs automation
* Confirm that the updates are visible at https://darshan.readthedocs.io/en/latest/

### Differences between the "latest" and "stable" versions

On the readthedocs dashboard, there are two items shown under tab "Versions",
`latest` and `stable`.
* **latest** - When using the default configuration, the latest version is a
  built whenever a new commit is pushed to the Github main branch. Therefore,
  the document pointed by **latest** is updated more frequently. Click it will
  reveal all the build history.
* **stable** - This points to the document that was built using the latest git
  tag, which is usually considered as a stable release of a software.
  + Note that readthedocs requires tag names to follow the semantic versioning,
    so it can be recognized by readthedocs.
    See [Section Versions](https://docs.readthedocs.com/platform/latest/versions.html)
    of ReadtheDocs document.


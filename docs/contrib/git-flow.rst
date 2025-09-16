.. default-role:: literal

Git flow
########

Branching model is inspired by `Git branching model by Vincent Driessen`, but a
simplified version.

Vincent recommends having two branches `develop` and `master`, we don't use
`develop`, we use only `master`.

List of flow steps:

1. Create a feature branch from `master`.
2. Merge feature branches to `master`.
3. Create a release branch, for example `0.2`.
4. Release a release candidate version, for example `0.2rc1`.
5. Release a new release candidate version if previous version had bugs, for
   example `0.2.rc2`.
6. Release a new version, from latest release candidate, for example `0.2`.
7. Create a bug fix branch from a release branch, for example `42-fix-bug`.
8. Merge a bug fix branch back to release and master branches.
9. Release an new bug fix version, for example `0.2.1`.

List of version numbers:

- `{major}` - only used for major, possibly backwards incompatible changes.
- `{major}.{minor}` - used for minor, mostly backwards compatible changes.
- `{major}.{minor}rc{number}` - used as a pre-release, before releasing
  `{major}.{minor}` version.
- `{major}.{minor}.{patch}` - a bug fix release, no new features, only bug
  fixes.

Long lived branches:

- `master` - main development branch, with all the newest code.

- `{major}.{minor}` - release branches use for bug fixes.

Long lived tags:

- `{major}.{minor}rc{number}` - release candidates.
- `{major}.{minor}.{patch}` - bug fix releases.

Short lived branches:

- `{number}-{slug}` - feature branches, starting with issue number and a slug
  that is machine and human readable title of what this branch is about,
  usually this matches issue title, for example `634-mermaid-diagrams`,
  `652-add-geometry-type`. Use only ascii letters, separate words with hyphens.


Feature branches
****************

| May branch off from:
|     `master`
| Must merge back into:
|     `master`
| Branch naming convention:
|     `{number}-{slug}`

Feature branches must be associated with an issue and must start with an issue
number. One issue, might have multiple feature branches.

For each feature branch a `GitHub Pull request`_ must be created and `linked to
issue <gh-link-pr-to-issue_>`_.

Feature branches must be deleted after merge.

Creating a feature branch
=========================

You can create feature branch using `GitHub Web UI <gh-new-branch_>`_, or
command line::

    git checkout -b {number}-{slug} master

Merging a feature branch
========================

You can merge feature branch using `GitHub Web UI <gh-merge-pr_>`_, or command
line::

    git checkout master
    git merge --no-ff {number}-{slug}
    git branch -d {number}-{slug}
    git push origin master

This will merge feature branch to master, deletes feature branch and pushes
changes to GitHub.


Release branches
****************

| May branch off from:
|     `master`
| Must merge back into:
|     `master`
| Branch naming convention:
|     `{major}.{minor}`

Release branches are used for adding bug fixes to specific version, also for
release candidates for new release preparations.

At some point in time a new release branch might be created from `master`, to
freeze development and prepare new version for release.

Release branches can only be used for fixing bugs, new features must be added
to `master`.


Creating a release branch
=========================

New releases must always be release as release candidates using
`{major}.{minor}rc{number}` version number. But in `CHANGES.rst` we do not show
pre-release versions.

You can create release branch using `GitHub Web UI <gh-new-branch_>`_, or
command line::

    git checkout -b {major}-{minor} master

After that you need to prepare Spinta for a release candidate::

    ed pyproject.toml <<EOF
    /^version = /c
    version = "{major}.{minor}rc{number}"
    .
    wq
    EOF
    ed CHANGES.rst <<EOF
    /^###/a

    {major}.{minor} (unreleased)
    ===================

    .
    wq
    EOF
    head CHANGES.rst
    git diff
    git commit -a -m "Prepare for the next {major}.{minor} release"
    git push origin {major}-{minor}
    git log -n3

You can release multiple release candidates until enough testing is done, and
new version is considered stable and ready for publishing.

Release branches are never deleted.


Releasing a new version
***********************

Follow `notes/spinta/release.sh` notes for releasing new versions.

When a new version is released it must be tagged with version number, in order
to mark a point in code history, that was released under a version number.

Tags are not branches, and the are used as markers, they can't receive new
commits as branches do.

You can tag new versions using command line::

    git checkout {major}.{minor}
    git tag -a {major}.{minor}.{patch} -m "Releasing version {major}.{minor}.{patch}"
    git push origin {major}.{minor}

After version is tagged, `create a GitHub release`_.


Creating bug fix branches
*************************

| May branch off from:
|     `{major}.{minor}`
| Must merge back into:
|     `{major}.{minor}` and `master`
| Branch naming convention:
|     `{number}-{slug}`

Bug fix branches are similar to feature branches, only they are branched off
from a release branch.

Multiple bug fixes can be added to a release branch and released under a single
release candidate or patch version.

Creating a feature branch
=========================

You can create feature branch using `GitHub Web UI <gh-new-branch_>`_, or
command line::

    git checkout -b {number}-{slug} {major}.{minor}

Merging a feature branch
========================

You can merge a bug fix branch using `GitHub Web UI <gh-merge-pr_>`_, or
command line::

    git checkout {major}.{minor}
    git merge --no-ff {number}-{slug}
    git branch -d {number}-{slug}
    git push origin {major}.{minor}

Also you need to add changes back to master::

    git checkout master
    git merge --no-ff {major}.{minor}
    git push origin master

This will merge bug fix branch to release and master branches, deletes bug fix
branch and pushes changes to GitHub.







.. _create a GitHub release: https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository
.. _gh-merge-pr: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/merging-a-pull-request
.. _gh-link-pr-to-issue: https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue
.. _GitHub Pull request: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request
.. _gh-new-branch: https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-a-branch-for-an-issue
.. _Git branching model by Vincent Driessen: https://nvie.com/posts/a-successful-git-branching-model/



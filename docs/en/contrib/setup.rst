.. default-role:: literal

#######################
Development environment
#######################

System requirements
*******************

Spinta is mostly developed on Linux, so you need a Linux or at least a Mac. If
you are using Windows, then you can use Windows subsystem for Linux. Of course,
if you are brave enough, you can add native Windows support too, but this might
take some time.

You must have Python_. The exact version should match the one pinned in the
repository's ``flake.nix`` (see section :ref:`nix-develop` below).

You need Poetry_ package manager.

Alternatively, if you have Nix_, you can get Python_ (the version pinned in
``flake.nix``), Poetry_, ``make``
and other tools preconfigured via a development shell, see section
:ref:`nix-develop` below. Docker is not provided by the Nix environment and
has to be installed on the host system.

Get your favorite code editor. This project was developed using mainly NeoVim_
and PyCharm_, but you can use any code editor you like.

.. _nix-develop:

Using Nix
*********

Nix is a package manager that works on Linux (including NixOS) and macOS. On
NixOS it is preinstalled; elsewhere, follow the official installation
instructions at https://nixos.org/download, or on most Linux distributions
(including Ubuntu, Debian, Fedora and macOS) use the official install
script::

    sh <(curl -L https://nixos.org/nix/install) --daemon

After the installation, restart your shell (or re-login) so that the ``nix``
command is on ``PATH``.

The repository includes a ``flake.nix`` describing a development environment
with all required tools (Python_, Poetry_, ``make``, nushell, ``curl`` and
libraries required by Python packages). The Python version is pinned in
``flake.nix`` (``pkgs.python3``), so when Python is upgraded there, this
documentation needs no changes. The Python packages themselves are
installed into a local ``.venv`` directory using Poetry, see
:ref:`development-environment` below for details.

Note that the Nix environment does *not* provide Docker -- it must be
installed on the host system, separately. Also make sure you can run Docker
without ``sudo``, for example by adding your user to the ``docker`` group
(see `the official Docker installation instructions
<https://docs.docker.com/engine/install/#server>`_).

To enter this environment, run from the repository root::

    nix develop path:.

This drops you into a shell where all project Makefile targets (``make``,
``make test``, ``make format``, and so on) work as expected.

Prefer ``nix develop path:.`` over plain ``nix develop`` -- the latter always
uses the committed version of the repository, so any uncommitted changes to
``flake.nix`` or other files affecting the Nix evaluation will not be visible
inside the dev shell. The ``path:.`` form always uses the repository's current
state, even with uncommitted changes.

To run a single command in that environment without entering an interactive
shell, prefix it with ``nix develop path:. -c``, for example::

    nix develop path:. -c make test

.. _development-environment:

Development environment
***********************

Clone code repository locally. Clone it to the ``spinta/master``
directory, so all further branches could be checked out as git worktrees
in the ``spinta/`` directory. This way each branch will get its own
directory next to the main ``master`` directory, while sharing the same
git object database::

    git clone git@github.com:atviriduomenys/spinta.git spinta/master

Check out the branch you are going to work on as a git worktree::

    cd spinta/master
    git worktree add ../<branch>  # checkout an existing branch
    git worktree add -b <branch> ../<branch>  # or create a new branch

If the branch you want to check out does not exist locally yet, but was
fetched from the remote, base the new worktree on it::

    git worktree add -b <branch> ../<branch> origin/<remote-branch>

From now on work inside the worktree of the branch you are working on.
Git commands work there as usual -- data between worktrees is shared,
only checked out files are separate. Use ``git worktree list`` to see
all worktrees, and ``git worktree remove ../<branch>`` to remove a
worktree once the branch is no longer needed.

Prepare project for work::

    make

Run services needed for tests::

    docker-compose up -d

Run tests::

    pytest -vvx --tb=short tests

That should be it, you are ready to contribute some code!


Contributing code
*****************

Before starting, first pick an issue_, or create a new one.

Create a new branch for the issue and check it out as a new worktree (see
:ref:`development-environment` above)::

    git worktree add -b <branch> ../<branch>

Write tests first.

Work on a solution.

Submit a pull request.

Get a code review.

Done.


.. _Python: https://www.python.org/
.. _Poetry: https://python-poetry.org/docs/
.. _Nix: https://nixos.org/download
.. _NeoVim: http://neovim.io/
.. _PyCharm: https://www.jetbrains.com/pycharm/
.. _issue: https://github.com/atviriduomenys/spinta/issues

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

You must have Python_ 3.10 or newer.

You need Poetry_ package manager.

Get your favorite code editor. This project was developed using mainly NeoVim_
and PyCharm_, but you can use any code editor you like.

Development environment
***********************

Clone code repository locally::

    git clone git@github.com:atviriduomenys/spinta.git && cd spinta

Prepare project for work::

    make

Run services needed for tests::

    docker-compose up -d

Run tests::

    poetry run pytest -vvx --tb=short tests

That should be it, you are ready to contribute some code!


Contributing code
*****************

Before starting, first pick an issue_, or create a new one.

Create a new branch for the issue.

Checkout your issue branch.

Write tests first.

Work on a solution.

Submit a pull request.

Get a code review.

Done.


.. _Python: https://www.python.org/
.. _Poetry: https://python-poetry.org/docs/
.. _NeoVim: http://neovim.io/
.. _PyCharm: https://www.jetbrains.com/pycharm/
.. _issue: https://github.com/atviriduomenys/spinta/issues

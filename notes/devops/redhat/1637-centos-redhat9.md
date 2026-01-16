# Spinta. Issue [#1637](https://github.com/atviriduomenys/spinta/issues/1637)

# Install Pyenv

```bash
yum install git
```

```bash
cat /etc/os-release
#| ((.venv) ) [spinta@cs9 ~]$ cat /etc/os-release
#| NAME="CentOS Stream"
#| VERSION="9"
#| ID="centos"
#| ID_LIKE="rhel fedora"
#| VERSION_ID="9"
#| PLATFORM_ID="platform:el9"
#| PRETTY_NAME="CentOS Stream 9"
#| ANSI_COLOR="0;31"
#| LOGO="fedora-logo-icon"
#| CPE_NAME="cpe:/o:centos:centos:9"
#| HOME_URL="https://centos.org/"
#| BUG_REPORT_URL="https://issues.redhat.com/"
#| REDHAT_SUPPORT_PRODUCT="Red Hat Enterprise Linux 9"
#| REDHAT_SUPPORT_PRODUCT_VERSION="CentOS Stream"
yum update
yum install python3.12 python3.12-pip
# add user
adduser spinta
# loggin to user with less privileges
sudo su - spinta
# check home directory
pwd
# /home/spinta or /opt/spinta
#
# create virtual environmet for application
python3.12 -m venv .venv
# check if virtual environment is created
ls -la .venv
#| [spinta@cs9 ~]$ ls -al .venv/
#| total 8
#| drwxr-xr-x 5 spinta spinta   7 Dec 17 00:06 .
#| drwx------ 8 spinta spinta  13 Dec 17 00:18 ..
#| drwxr-xr-x 3 spinta spinta  50 Dec 17 00:08 bin
#| drwxr-xr-x 4 spinta spinta   4 Dec 17 00:08 include
#| drwxr-xr-x 3 spinta spinta   3 Dec 17 00:06 lib
#| lrwxrwxrwx 1 spinta spinta   3 Dec 17 00:06 lib64 -> lib
#| -rw-r--r-- 1 spinta spinta 161 Dec 17 00:06 pyvenv.cfg
#|
# activate virtual environment
source .venv/bin/activate
# check python version
python --version
#| Python 3.12.1
#
# upgrade pip
pip install --upgrade pip
# install spinta
pip install --pre spinta
spinta --version
mkdir test
```

## Install Python3.13

Broken RHEL9 implementation. No `cimmutabledict` module for SqlAlchemy.

```bash
# enable epel
dnf install 'dnf-command(config-managery)'
dnf config-manager --set-enabled crb
dnf install https://dl.fedoraproject.org/pub/epel/epel{,-next}-release-latest-9.noarch.rpm
# install python3.13
dnf install python3.13 python3.13-pip
# loggin to user with less privileges
sudo su - spinta
# create virtual environment
python3.13 -m venv .venv313
# activate virtual environment
source .venv313/bin/activate
# check python version
python --version
#| Python 3.13.0a4
pip --version
#| pip 25.3 from /home/spinta/.venv313/lib64/python3.13/site-packages/pip (python 3.13)
# upgrade pip
pip install --upgrade pip
# install spinta
pip install --pre spinta
spinta --version
```

## Solution for Python3.13

There is no module `cimmutabledict` in `Python3.13` and `SqlAlchemy 1.4.54`. But exists in `SqlAlchemy 1.4.53`!!!

```bash
pip install sqlalchemy==1.4.53
```

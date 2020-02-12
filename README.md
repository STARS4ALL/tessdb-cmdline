# tessdb command line (overview)

Linux command line utility.  TESS stands for [Cristobal Garcia's Telescope Encoder and Sky Sensor](http://www.observatorioremoto.com/TESS.pdf)
It is being used as part of the [STARS4ALL Project](http://www.stars4all.eu/).

## Description

`tess` is a Linux command line utility to perform some common operations on the TESS database without having to write SQL statements. As this utility modifies the database, it is necessary to invoke it within using `sudo`. Also, you should ensure that the database is not being written by `tessdb` systemd service to avoid *database is locked* exceptions, either by using it at daytime or by pausing the `tessdb` systemd service with `/usr/local/bin/tessdb_pause` and then resume it with `/usr/local/bin/tessdb_resume`.


# INSTALLATION
    
## Requirements

The following components are needed and should be installed first:

 * python 2.7.x (tested on Ubuntu Python 2.7.6) or python 3.6+

### Installation

Installation is done from GitHub:

    git clone https://github.com/astrorafael/tessdb-cmdline.git
    cd tess-cmdline
    sudo python setup.py install

**Note:** Installation from PyPi is now obsolete. Do not use the package uploaded in PyPi.

* All executables are copied to `/usr/local/bin`
* The database is located at `/var/dbase/tess.db` by default, although a diffferent path may be specified.

# DATA MODEL

## Dimensional Modelling

The data model follows the [dimensional modelling]
(https://en.wikipedia.org/wiki/Dimensional_modeling) approach by Ralph Kimball. More references can also be found in [Star Schemas](https://en.wikipedia.org/wiki/Star_schema).

## The data model

The latest version of the data model can be found in **tessdb-server** repository.

![TESS Database Model](https://github.com/STARS4ALL/tessdb/raw/master/doc/tessdb-full.png) 

# COMMANDS

The `tess` command line tool is self-explanatory and has several subcommands. You can find the all by typing `tess --help`
```
pi@rb-tess:~ $ tess --help
usage: /usr/local/bin/tess [-h] {instrument,location,readings} ...

positional arguments:
  {instrument,location,readings}
    instrument          instrument commands
    location            location commands
    readings            readings commands

optional arguments:
  -h, --help            show this help message and exit
```

Each subcommand has its own help that you may display by issuing `tess <subcommand> --help`

Example:
```
pi@rb-tess:~ $ tess location list --help
usage: /usr/local/bin/tess location list [-h] [-p PAGE_SIZE] [-d DBASE]

optional arguments:
  -h, --help            show this help message and exit
  -p PAGE_SIZE, --page-size PAGE_SIZE
                        list page size
  -d DBASE, --dbase DBASE
                        SQLite database full file path
```

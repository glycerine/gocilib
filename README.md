# Install #
`go get github.com/tgulacsi/gocilib`

## Oracle DB ##
You will need an Oracle DB to connect to, with its libraries
[Oracle DB](http://www.oracle.com/technetwork/database/enterprise-edition/index.html) installed
OR the Oracle's
[InstantClient](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html)
*both* the Basic Client and the SDK (for the header files), too!

## OCILIB ##
  1. Download OCILIB from http://sourceforge.net/projects/orclib/files/latest/download
  OR just use the vendored [version](./third_party/ocilib)

  1. `tar xaf ocilib-3.12.1.tar.gz && cd ocilib-3.12.1`

  1. environment variables: you can try [env](./env)
  or see Environment Variables below.

  1. `./configure --with-oracle-charset=ansi --prefix=/usr/local`
  or (with instantclient at its default locations)
  `./configure --with-oracle-charset=ansi --with-oracle-headers-path=/usr/include/oracle/11.2/client64 --with-oracle-lib-path=/usr/lib/oracle/11.2/client64/lib --prefix=/usr/local`

  1. `make && sudo make install && sudo ldconfig`


# Environment variables #
[env](./env)

## Linux ##
AND you have set proper environment variables:

    export CGO_CFLAGS=-I$(dirname $(find $ORACLE_HOME -type f -name oci.h))
    export CGO_LDFLAGS=-L$(dirname $(find $ORACLE_HOME -type f -name libclntsh.so\*))
    go get github.com/tgulacsi/gocilib/godrv

For example, with my [XE](http://www.oracle.com/technetwork/products/express-edition/downloads/index.html):

    ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
    CGO_CFLAGS=-I/u01/app/oracle/product/11.2.0/xe/rdbms/public
    CGO_LDFLAGS=-L/u01/app/oracle/product/11.2.0/xe/lib

With InstantClient:

    CGO_CFLAGS=-I/usr/include/oracle/11.2/client64
    CGO_LDFLAGS=-L/usr/include/oracle/11.2/client64

## Mac OS X ##
For Mac OS X I did the following:

You have to get both the Instant Client Package Basic and the Instant Client Package SDK (for the header files).

Then set the env vars as this (note the SDK here was unpacked into the base directory of the Basic package)

    export CGO_CFLAGS=-I/Users/dfils/src/oracle/instantclient_11_2/sdk/include
    export CGO_LDFLAGS=-L/Users/dfils/src/oracle/instantclient_11_2
    export DYLD_LIBRARY_PATH=/Users/dfils/src/oracle/instantclient_11_2:$DYLD_LIBRARY_PATH

Perhaps this export would work too, but I did not try it.  I understand this is another way to do this

    export DYLD_FALLBACK_LIBRARY_PATH=/Users/dfils/src/oracle/instantclient_11_2

The DYLD vars are needed to run the binary, not to compile it.

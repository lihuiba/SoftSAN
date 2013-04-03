#!/bin/sh
yum install -y protobuf-compiler protobuf-python redis python-redis libevent-devel iscsi-initiator-utils scsi-target-utils
easy_install python-greenlet gevent

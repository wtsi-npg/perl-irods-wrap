#!/bin/bash

sudo rabbitmqctl add_vhost test_vhost
sudo rabbitmqctl add_user test_user p455w0rd
sudo rabbitmqctl set_user_tags test_user administrator
sudo rabbitmqctl set_permissions -p test_vhost test_user ".*" ".*" ".*"

cpanm --local-lib=~/perl5 local::lib && eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)

./scripts/rabbitmq_config.pl # sets up queues and exchanges

sudo rabbitmqctl status
sudo rabbitmqctl list_exchanges -p test_vhost
sudo rabbitmqctl list_queues -p test_vhost
sudo rabbitmqctl list_bindings -p test_vhost

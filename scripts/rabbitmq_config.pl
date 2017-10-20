#! /usr/bin/env perl

use warnings;
use strict;

use File::Slurp qw/read_file/;
use JSON;
use Net::AMQP::RabbitMQ;

my $channel = 1;
my $exchange_gateway = 'npg.gateway';
my $exchange_activity = 'npg.activity';
my $queue = 'test_irods_data_create_messages';

my $rmq = Net::AMQP::RabbitMQ->new();

my $host = $ENV{'NPG_RMQ_HOST'};
my $opts = from_json(read_file($ENV{'NPG_RMQ_CONFIG'}));

$rmq->connect($host, $opts);

# We want to increment the channel number for each test
# So we declare 100 channels (many more than currently needed)

while ($channel <= 100) {

    $rmq->channel_open($channel);
    $rmq->exchange_declare(
        $channel,
        $exchange_gateway,
        { exchange_type => 'fanout' },
    );
    $rmq->exchange_declare(
        $channel,
        $exchange_activity,
        { exchange_type => 'topic' },
    );
    $rmq->queue_declare(
        $channel,
        $queue,
    );
    $rmq->exchange_bind(
        $channel,
        $exchange_activity,
        $exchange_gateway,
        'test.irods.*',
    );
    $rmq->queue_bind(
        $channel,
        $queue,
        $exchange_activity,
        'test.irods.*',
    );

    $channel++;
}

$rmq->disconnect();

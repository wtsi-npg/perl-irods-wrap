package WTSI::NPG::iRODS::ReporterTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use Log::Log4perl;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::iRODS::TestRabbitMQ];

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $log = Log::Log4perl::get_logger();

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = './t/data/reporter';

my @header_keys = qw[timestamp
                     user
                     irods_user
                     type
                     method];
my $expected_headers = scalar @header_keys;

my $irods_tmp_coll;
my $remote_file_path;
my $cwc;
my $test_host = $ENV{'NPG_RMQ_HOST'} || 'localhost';
my $conf = $ENV{'NPG_RMQ_CONFIG'} || './etc/rmq_test_config.json';
my $queue = 'test_irods_data_create_messages';

my $irods_class      = 'WTSI::NPG::TestMQiRODS';
my $subscriber_class = 'WTSI::NPG::RabbitMQ::TestSubscriber';

# Net::AMQP::RabbitMQ is not required to be installed
eval "require $irods_class";
eval "require $subscriber_class";

# Each test has a channel number, equal to $test_counter. The channel
# is used by the publisher (iRODS instance) and subscriber in that test only.

sub setup_test : Test(setup) {
    # Clear the message queue. For a given run of the test harness, each
    # test has its own RabbitMQ channel; but messages may persist between runs
    # in a given queue and channel, eg. from previous failed tests.
    # (Assigning a unique queue name would need reconfiguration of the
    # RabbitMQ test server.)
    $test_counter++;
    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    # messaging disabled for test setup
    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  no_rmq               => 1,
                                 );
    $cwc = $irods->working_collection;
    $irods_tmp_coll =
        $irods->add_collection("PublisherTest.$pid.$test_counter");
    $remote_file_path = "$irods_tmp_coll/lorem.txt";
    $irods->add_object("$data_path/lorem.txt", $remote_file_path);
}

sub teardown_test : Test(teardown) {
    # messaging disabled for test teardown
    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  no_rmq               => 1,
                                 );
    $irods->working_collection($cwc);
    $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::Publisher');
}

### collection tests ###

sub test_add_collection : Test(14) {
    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );

    my $irods_new_coll = $irods_tmp_coll.'/temp';
    $irods->add_collection($irods_new_coll);

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'add_collection';
    _test_collection_message($message, $method);
}

sub test_collection_avu : Test(43) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );

    $irods->add_collection_avu($irods_tmp_coll, 'colour', 'green');
    $irods->add_collection_avu($irods_tmp_coll, 'colour', 'purple');
    $irods->remove_collection_avu($irods_tmp_coll, 'colour', 'green');

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 3, 'Got 3 messages from queue');

    my @methods = qw[add_collection_avu
                     add_collection_avu
                     remove_collection_avu];
    my $i = 0;

    my $purple =  {
        'attribute' => 'colour',
        'value' => 'purple'
    };
    my $green =  {
        'attribute' => 'colour',
        'value' => 'green'
    };
    my @expected_avus = (
        [$green],
        [$purple, $green],
        [$purple]
    );

    foreach my $message (@messages) {
        my ($body, $headers) = @{$message};
        _test_collection_message($message, $methods[$i]);
        is_deeply($body->{'avus'}, $expected_avus[$i]);
        $i++;
    }
}

sub test_put_move_collection : Test(27) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );

    $irods->put_collection($data_path, $irods_tmp_coll);
    my $dest_coll = $irods_tmp_coll.'/reporter';
    my $moved_coll = $irods_tmp_coll.'/reporter.moved';
    $irods->move_collection($dest_coll, $moved_coll);

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    my $i = 0;
    my @methods = qw[put_collection move_collection];

    foreach my $message (@messages) {
        _test_collection_message($message, $methods[$i]);
        $i++;
    }
}

sub test_remove_collection : Test(14) {
    my $irods_no_rmq = $irods_class->new(environment          => \%ENV,
                                         strict_baton_version => 0,
                                         no_rmq               => 1,
                                        );
    my $irods_new_coll = $irods_tmp_coll.'/temp';
    $irods_no_rmq->add_collection($irods_new_coll);

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );
    $irods->remove_collection($irods_new_coll);
    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'remove_collection';
    _test_collection_message($message, $method);
}

sub test_set_collection_permissions : Test(27) {
    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );
    my $user = 'public';
    $irods->set_collection_permissions($WTSI::NPG::iRODS::NULL_PERMISSION,
                                       $user,
                                       $irods_tmp_coll,
                                   );
    $irods->set_collection_permissions($WTSI::NPG::iRODS::OWN_PERMISSION,
                                       $user,
                                       $irods_tmp_coll,
                                   );

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    my $method = 'set_collection_permissions';

    foreach my $message (@messages) {
        _test_collection_message($message, $method);
    }
}


### data object tests ###

sub test_add_object : Test(18) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );
    my $added_remote_path = "$irods_tmp_coll/lorem_copy.txt";
    $irods->add_object("$data_path/lorem.txt", $added_remote_path);

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    _test_object_message($message, 'add_object');
    my ($body, $headers) = @{$message};
    # temporary staging object is named lorem_copy.txt.[suffix]
    ok($body->{'data_object'} =~ /^lorem_copy\.txt/msx,
       'Data object name starts with lorem_copy.txt');
    ok($body->{'collection'} eq $irods_tmp_coll,
       "Collection name is $irods_tmp_coll");
}

sub test_copy_object : Test(18) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                );
    my $copied_remote_path = "$irods_tmp_coll/lorem_copy.txt";
    $irods->copy_object($remote_file_path, $copied_remote_path);

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    _test_object_message($message, 'copy_object');

    my ($body, $headers) = @{$message};
    ok($body->{'data_object'} eq 'lorem_copy.txt',
       'Data object name is lorem_copy.txt');
    ok($body->{'collection'} eq $irods_tmp_coll,
       "Collection name is $irods_tmp_coll");
}

sub test_move_object : Test(18) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );
    my $moved_remote_path = "$irods_tmp_coll/lorem_moved.txt";
    $irods->move_object($remote_file_path, $moved_remote_path);

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    _test_object_message($message, 'move_object');

    my ($body, $headers) = @{$message};
    ok($body->{'data_object'} eq 'lorem_moved.txt',
       'Data object name is lorem_moved.txt');
    ok($body->{'collection'} eq $irods_tmp_coll,
       "Collection name is $irods_tmp_coll");
}

sub test_object_avu : Test(55) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );

    $irods->add_object_avu($remote_file_path, 'colour', 'green');
    $irods->add_object_avu($remote_file_path, 'colour', 'purple');
    $irods->remove_object_avu($remote_file_path, 'colour', 'green');

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 3, 'Got 3 messages from queue');

    my @methods = qw[add_object_avu add_object_avu remove_object_avu];
    my $i = 0;
    my $purple =  {
        'attribute' => 'colour',
        'value' => 'purple'
    };
    my $green =  {
        'attribute' => 'colour',
        'value' => 'green'
    };
    my @expected_avus = (
        [$green],
        [$purple, $green],
        [$purple]
    );

    foreach my $message (@messages) {
        _test_object_message($message, $methods[$i]);
        my ($body, $headers) = @{$message};
        is_deeply($body->{'avus'}, $expected_avus[$i]);
        # temporary staging object is named lorem.txt.[suffix]
        ok($body->{'data_object'} eq 'lorem.txt',
           'Data object name is lorem.txt');
        ok($body->{'collection'} eq $irods_tmp_coll,
           "Collection name is $irods_tmp_coll");
        $i++;
    }
}

sub test_remove_object : Test(16) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );
    $irods->remove_object($remote_file_path);
    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'remove_object';
    _test_object_message($message, $method);
}

sub test_replace_object : Test(18) {

    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );
    $irods->replace_object("$data_path/lorem.txt", $remote_file_path);

    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    _test_object_message($message, 'replace_object');
    my ($body, $headers) = @{$message};
    # temporary staging object is named lorem.txt.[suffix]
    ok($body->{'data_object'} =~ /^lorem\.txt/msx,
       'Data object name starts with lorem.txt');
    ok($body->{'collection'} eq $irods_tmp_coll,
       "Collection name is $irods_tmp_coll");
}

sub test_set_object_permissions : Test(31) {
    # change permissions on a data object, with messaging
    my $irods = $irods_class->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  routing_key_prefix   => 'test',
                                  hostname             => $test_host,
                                  rmq_config_path      => $conf,
                                  channel              => $test_counter,
                                 );
    my $user = 'public';
    $irods->set_object_permissions($WTSI::NPG::iRODS::NULL_PERMISSION,
                                   $user,
                                   $remote_file_path,
                               );
    $irods->set_object_permissions($WTSI::NPG::iRODS::OWN_PERMISSION,
                                   $user,
                                   $remote_file_path,
                               );
    my $subscriber_args = _get_subscriber_args($test_counter);
    my $subscriber = $subscriber_class->new($subscriber_args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');
    my $method = 'set_object_permissions';

    foreach my $message (@messages) {
        _test_object_message($message, $method);
    }
}

### methods for repeated tests ###

sub _get_subscriber_args {
    my ($channel, ) = @_;
    my $args = {
        hostname             => $test_host, # global variable
        rmq_config_path      => $conf,      # global variable
        channel              => $channel,
    };
    return $args;
}

sub _test_collection_message {
    my ($message, $method) = @_;
    # total tests = 11
    my @body_keys = qw[collection
                       access
                       avus
                       timestamps];
    return _test_message($message, $method, \@body_keys);
}

sub _test_object_message {
    my ($message, $method) = @_;
    # total tests = 13
    my @body_keys = qw[collection
                       data_object
                       access
                       avus
                       replicates
                       timestamps];
    return _test_message($message, $method, \@body_keys);
}

sub _test_message {
    my ($message, $method, $body_keys) = @_;
    # total tests = 7 + number of body keys
    #             = 13 for object, 11 for collection
    my ($body, $headers) = @{$message};
    my @body_keys = @{$body_keys};
    my $expected_body_obj = scalar @body_keys;
    ok(scalar keys(%{$headers}) == $expected_headers,
       'Found '.$expected_headers.' header key/value pairs.');
    ok(scalar keys(%{$body}) == $expected_body_obj,
       'Found '.$expected_body_obj.' body key/value pairs.');
    ok($headers->{'method'} eq $method, 'Method name is '.$method);
    my $time = $headers->{'timestamp'};
    ok($time =~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}/msx,
       "Header timestamp '$time' is in correct format");
    foreach my $key (@header_keys) {
        ok(defined $headers->{$key},
           'Value defined in message header for '.$key);
    }
    foreach my $key (@body_keys) {
        ok(defined $body->{$key},
           'Value defined in message body for '.$key);
    }

}

1;

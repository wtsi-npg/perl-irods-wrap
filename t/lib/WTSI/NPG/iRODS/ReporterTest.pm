package WTSI::NPG::iRODS::ReporterTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use JSON;
use Log::Log4perl;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::iRODS::TestRabbitMQ];

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $log = Log::Log4perl::get_logger();

use WTSI::NPG::iRODSMQTest;
use WTSI::NPG::PublisherMQTest;
use WTSI::NPG::RabbitMQ::TestCommunicator;

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = './t/data/reporter';

my @header_keys = qw[timestamp
                     user
                     irods_user
                     type
                     method];
my $expected_headers = scalar @header_keys;

my $test_filename = 'lorem.txt';
my $irods_tmp_coll;
my $remote_file_path;
my $cwc;
my $test_host = $ENV{'NPG_RMQ_HOST'} || 'localhost';
my $conf = $ENV{'NPG_RMQ_CONFIG'} || './etc/rmq_test_config.json';
my $queue = 'test_irods_data_create_messages';

# Each test has a channel number, equal to $test_counter. The channel
# is used by the publisher (iRODS instance) and subscriber in that test only.
# Each channel *must* be declared in the RabbitMQ server configuration;
# see scripts/rabbitmq_config.pl for an example.

sub setup_test : Test(setup) {
    # Clear the message queue. For a given run of the test harness, each
    # test has its own RabbitMQ channel; but messages may persist between runs
    # in a given queue and channel, eg. from previous failed tests.
    # (Assigning a unique queue name would need reconfiguration of the
    # RabbitMQ test server.)
    $test_counter++;
    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    # messaging disabled for test setup
    my $irods = WTSI::NPG::iRODSMQTest->new(environment          => \%ENV,
                        strict_baton_version => 0,
                        enable_rmq           => 0,
                       );

    $cwc = $irods->working_collection;
    $irods_tmp_coll =
        $irods->add_collection("PublisherTest.$pid.$test_counter");
    $remote_file_path = "$irods_tmp_coll/$test_filename";
    $irods->add_object("$data_path/$test_filename", $remote_file_path);
}

sub teardown_test : Test(teardown) {
    # messaging disabled for test teardown
    my $irods = WTSI::NPG::iRODSMQTest->new(environment          => \%ENV,
                        strict_baton_version => 0,
                        enable_rmq           => 0,
                       );
    $irods->working_collection($cwc);
    $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::Publisher');
}

sub test_message_queue : Test(2) {
    # ensure the test message queue is working correctly
    my $args = _get_subscriber_args($test_counter);
    my $subscriber =  WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my $body = ["Hello, world!", ];
    $subscriber->publish(encode_json($body),
                         'npg.gateway',
                         'test.irods.report'
                     );
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');
    my $message = shift @messages;
  SKIP: {
        # If message undefined, skip tests on content to improve readability
        # Distinct from option to skip all RabbitMQ tests; see TestRabbitMQ.pm
        skip "RabbitMQ message not defined", 1 if not defined $message;
        my ($msg_body, $msg_headers) = @{$message};
        is_deeply($msg_body, $body, 'Message body has expected value');
    }
}

### collection tests ###

sub test_add_collection : Test(11) {
    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my $irods_new_coll = $irods_tmp_coll.'/temp';
    $irods->add_collection($irods_new_coll);

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'add_collection';
    my @acl = $irods->get_collection_permissions($irods_new_coll);
    my $body = {avus       => [],
                acl        => \@acl,
        collection => $irods_new_coll,
           };
    _test_collection_message($message, $method, $body, $irods);
    $irods->rmq_disconnect();
}

sub test_collection_avu : Test(31) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    $irods->add_collection_avu($irods_tmp_coll, 'colour', 'green');
    $irods->add_collection_avu($irods_tmp_coll, 'colour', 'purple');
    $irods->remove_collection_avu($irods_tmp_coll, 'colour', 'green');

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    my $expected_messages = 3;
    is(scalar @messages, $expected_messages, 'Got 3 messages from queue');

    my @methods = qw[add_collection_avu
                     add_collection_avu
                     remove_collection_avu];
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
        [$green, $purple],
        [$purple]
    );
    my @acl = $irods->get_collection_permissions($irods_tmp_coll);
    my $i = 0;
    while ($i < $expected_messages ) {
    my $body = {avus       => $expected_avus[$i],
                    acl        => \@acl,
            collection => $irods_tmp_coll,
           };
        _test_collection_message($messages[$i], $methods[$i], $body, $irods);
        $i++;
    }
    $irods->rmq_disconnect();
}

sub test_put_move_collection : Test(21) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    $irods->put_collection($data_path, $irods_tmp_coll);
    my $dest_coll = $irods_tmp_coll.'/reporter';
    my @acl = $irods->get_collection_permissions($dest_coll);
    my $put_body = {avus       => [],
                    acl        => \@acl,
            collection => $dest_coll,
           };
    my $moved_coll = $irods_tmp_coll.'/reporter.moved';
    $irods->move_collection($dest_coll, $moved_coll);
    # should have same permissions on put and moved collections
    my $moved_body = {avus       => [],
                      acl        => \@acl,
                      collection => $moved_coll,
           };

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    _test_collection_message($messages[0],
                 'put_collection',
                 $put_body,
                 $irods);

    _test_collection_message($messages[1],
                 'move_collection',
                 $moved_body,
                 $irods);

    $irods->rmq_disconnect();
}

sub test_remove_collection : Test(11) {
    my $irods_no_rmq = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       enable_rmq           => 0,
      );
    my $irods_new_coll = $irods_tmp_coll.'/temp';
    $irods_no_rmq->add_collection($irods_new_coll);

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my @acl = $irods->get_collection_permissions($irods_new_coll);
    $irods->remove_collection($irods_new_coll);
    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'remove_collection';
    my $body = {avus       => [],
                acl        => \@acl,
        collection => $irods_new_coll,
           };
    _test_collection_message($message, $method, $body, $irods);
    $irods->rmq_disconnect();
}

sub test_set_collection_permissions : Test(21) {
    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my $user = 'public';
    $irods->set_collection_permissions($WTSI::NPG::iRODS::NULL_PERMISSION,
                                       $user,
                                       $irods_tmp_coll,
                                   );
    my @acl_null = $irods->get_collection_permissions($irods_tmp_coll);
    my $body_null = {avus       => [],
                     acl        => \@acl_null,
                     collection => $irods_tmp_coll,
                 };
    $irods->set_collection_permissions($WTSI::NPG::iRODS::OWN_PERMISSION,
                                       $user,
                                       $irods_tmp_coll,
                                   );
    my @acl_own = $irods->get_collection_permissions($irods_tmp_coll);
    my $body_own = {avus       => [],
                    acl        => \@acl_own,
                    collection => $irods_tmp_coll,
                };

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    my $method = 'set_collection_permissions';
    _test_collection_message($messages[0], $method, $body_null, $irods);
    _test_collection_message($messages[1], $method, $body_own, $irods);

    $irods->rmq_disconnect();
}


### data object tests ###

sub test_add_object : Test(12) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my $copied_filename = 'lorem_copy.txt';
    my $added_remote_path = "$irods_tmp_coll/$copied_filename";
    $irods->add_object("$data_path/$test_filename", $added_remote_path);

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 1, 'Got 1 message from queue');
    my $message = shift @messages;
    my @acl = $irods->get_object_permissions($added_remote_path);
    my $body =  {avus        => [],
                 acl         => \@acl,
         collection  => $irods_tmp_coll,
         data_object => $copied_filename,
           };
    _test_object_message($message, 'add_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_copy_object : Test(12) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my $copied_filename = 'lorem_copy.txt';
    my $copied_remote_path = "$irods_tmp_coll/$copied_filename";
    $irods->copy_object($remote_file_path, $copied_remote_path);

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my @acl = $irods->get_object_permissions($copied_remote_path);
    my $body =  {avus        => [],
                 acl         => \@acl,
         collection  => $irods_tmp_coll,
         data_object => $copied_filename,
           };
    _test_object_message($message, 'copy_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_move_object : Test(12) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my $moved_filename = 'lorem_moved.txt';
    my $moved_remote_path = "$irods_tmp_coll/$moved_filename";
    $irods->move_object($remote_file_path, $moved_remote_path);

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my @acl = $irods->get_object_permissions($moved_remote_path);
    my $body =  {avus        => [],
                 acl         => \@acl,
         collection  => $irods_tmp_coll,
         data_object => $moved_filename,
           };
    _test_object_message($message, 'move_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_object_avu : Test(34) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    $irods->add_object_avu($remote_file_path, 'colour', 'green');
    $irods->add_object_avu($remote_file_path, 'colour', 'purple');
    $irods->remove_object_avu($remote_file_path, 'colour', 'green');

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    my $expected_messages = 3;
    is(scalar @messages, $expected_messages, 'Got 3 messages from queue');

    my @methods = qw[add_object_avu add_object_avu remove_object_avu];
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
        [$green, $purple],
        [$purple]
    );

    my @acl = $irods->get_object_permissions($remote_file_path);
    my $i = 0;
    while ($i < $expected_messages ) {
    my $body = {avus        => $expected_avus[$i],
                    acl         => \@acl,
            data_object => $test_filename,
            collection  => $irods_tmp_coll,
           };
        _test_object_message($messages[$i], $methods[$i], $body, $irods);
        $i++;
    }
    $irods->rmq_disconnect();
}

sub test_remove_object : Test(12) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my @acl = $irods->get_object_permissions($remote_file_path);
    $irods->remove_object($remote_file_path);
    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'remove_object';
    my $body =  {avus        => [],
                 acl         => \@acl,
         data_object => $test_filename,
         collection  => $irods_tmp_coll,
           };
    _test_object_message($message, $method, $body, $irods);
    $irods->rmq_disconnect();
}

sub test_replace_object : Test(12) {

    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    $irods->replace_object("$data_path/$test_filename", $remote_file_path);

    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my @acl = $irods->get_object_permissions($remote_file_path);
    my $body =  {avus        => [],
                 acl         => \@acl,
         data_object => $test_filename,
         collection  => $irods_tmp_coll,
           };
    _test_object_message($message, 'replace_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_set_object_permissions : Test(23) {
    # change permissions on a data object, with messaging
    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $irods->rmq_init();
    my $user = 'public';
    $irods->set_object_permissions($WTSI::NPG::iRODS::NULL_PERMISSION,
                                   $user,
                                   $remote_file_path,
                               );
    my @acl_null = $irods->get_object_permissions($remote_file_path);
    my $body_null = {avus        => [],
                     acl         => \@acl_null,
                     collection  => $irods_tmp_coll,
                     data_object => $test_filename,
                 };
    $irods->set_object_permissions($WTSI::NPG::iRODS::OWN_PERMISSION,
                                   $user,
                                   $remote_file_path,
                               );
    my @acl_own = $irods->get_object_permissions($remote_file_path);
    my $body_own = {avus        => [],
                    acl         => \@acl_own,
                    collection  => $irods_tmp_coll,
                    data_object => $test_filename,
                 };
    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');
    my $method = 'set_object_permissions';

    _test_object_message($messages[0], $method, $body_null, $irods);
    _test_object_message($messages[1], $method, $body_own, $irods);
    $irods->rmq_disconnect();
}

### methods for the Publisher class ###

sub test_publish_object : Test(14) {
    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       enable_rmq           => 0,
      );
    my $user = 'public';
    my $publisher = WTSI::NPG::PublisherMQTest->new
      (
       irods                => $irods,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $publisher->rmq_init();
    my $published_filename = 'ipsum.txt';
    my $remote_file_path = "$irods_tmp_coll/$published_filename";
    $remote_file_path = $irods->absolute_path($remote_file_path);
    my $pub_obj = $publisher->publish("$data_path/$test_filename",
                      $remote_file_path);
    ok($irods->is_object($remote_file_path), 'File published to iRODS');
    ok($remote_file_path eq $pub_obj->absolute()->str(),
       'Absolute data object paths from input and return value are equal');
    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');
    my $message = shift @messages;
    my $method = 'publish';
    my @acl = $irods->get_object_permissions($remote_file_path);
    my $body = {avus        => $pub_obj->get_metadata(),
        acl         => \@acl,
        collection  => $irods_tmp_coll,
        data_object => $published_filename,
           };
    _test_object_message($message, $method, $body, $irods);
    $publisher->rmq_disconnect();
}

sub test_publish_collection : Test(13) {
    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       enable_rmq           => 0,
      );
    my $user = 'public';
    my $publisher = WTSI::NPG::PublisherMQTest->new
      (
       irods                => $irods,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    $publisher->rmq_init();
    my $pub_coll = $publisher->publish($data_path, $irods_tmp_coll);
    my $dest_coll = $irods_tmp_coll.'/reporter';
    $dest_coll = $irods->absolute_path($dest_coll);
    ok($irods->is_collection($dest_coll), 'Collection published to iRODS');
    ok($dest_coll eq $pub_coll->absolute()->str(),
       'Absolute collection paths from input and return value are equal');
    my $args = _get_subscriber_args($test_counter);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    # get AVUs from iRODS collection to check against message body
    my $message = shift @messages;
    my $method = 'publish';
    my @acl = $irods->get_collection_permissions($dest_coll);
    my $body = {avus        => $pub_coll->get_metadata(),
        acl         => \@acl,
        collection  => $dest_coll,
           };
    _test_collection_message($message, $method, $body, $irods);
    $publisher->rmq_disconnect();
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
    my ($message, $method, $expected_body, $irods) = @_;
    # 10 tests in total
    return _test_message($message, $method, $expected_body, $irods, 0);
}

sub _test_object_message {
    my ($message, $method, $expected_body, $irods) = @_;
    # 11 tests in total
    return _test_message($message, $method, $expected_body, $irods, 1);
}

sub _test_message {
  # General-purpose method to test RabbitMQ messages.
  #
  # Arguments:
  # - [ArrayRef] RabbitMQ message, consisting of body and headers
  # - [Str] Method name
  # - [HashRef] Expected body of message.
  # - [WTSI::NPG::iRODS] iRODS object, used for sorting AVUs
  # - [Bool] Flag to indicate a data object (as opposed to a collection)
  #
  # Tests performed:
  # - Exact values of method, user, and irods_user headers
  # - Format of timestamp header
  # - Presence of file type header (value may be an empty string)
  # - Exact values of collection, data object and AVUs (if any) in body

  my ($message, $method, $expected_body, $irods, $is_data_object) = @_;
  my $expected_headers = 5; # timestamp, user, irods_user, type, method
  my $expected_body_keys_total = scalar keys(%{$expected_body});

  my $total_tests = 10;
  if ($is_data_object) { $total_tests++; }

  my $skip = not defined($message);
  if ($skip) {
    $log->logwarn('Unexpectedly got an undefined message from RabbitMQ; ',
          'skipping subsequent tests on content of the message');
  }
 SKIP: {
   # If message undefined, skip tests on content to improve readability
   # Distinct from option to skip all RabbitMQ tests; see TestRabbitMQ.pm
    skip "RabbitMQ message not defined", $total_tests if $skip;
    my ($body, $headers) = @{$message};

    # expected number of header/body fields
    ok(scalar keys(%{$headers}) == $expected_headers,
       'Found '.$expected_headers.' header key/value pairs.');
    ok(scalar keys(%{$body}) == $expected_body_keys_total,
       'Found '.$expected_body_keys_total.' body key/value pairs.');

    # check content of headers
    ok($headers->{'method'} eq $method, "Header method name is $method");
    my $time = $headers->{'timestamp'};
    ok($time =~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}/msx,
       "Header timestamp '$time' is in correct format");
    my $user = $ENV{'USER'};
    ok($headers->{'user'} eq $user, "Header user name is $user");
    ok($headers->{'irods_user'} eq $user, "Header iRODS user name is $user");
    ok(defined $headers->{'type'},
       "Header file type is defined (may be an empty string)");

    # check content of body
    ok($body->{'collection'} eq $expected_body->{'collection'},
       'Collection matches expected value');
    if ($is_data_object) {
      ok($body->{'data_object'} eq $expected_body->{'data_object'},
     'Data object matches expected value');
    }
    # sort AVUs to ensure consistent order for comparison
    my @avus = $irods->sort_avus(@{$body->{'avus'}});
    my @expected_avus = $irods->sort_avus(@{$expected_body->{'avus'}});
    is_deeply(\@avus, \@expected_avus, 'AVUs match expected values');
    # sort ACL to ensure consistent order for comparison
    my @acl = $irods->sort_acl(@{$body->{'acl'}});
    my @expected_acl = $irods->sort_acl(@{$expected_body->{'acl'}});
    is_deeply(\@acl, \@expected_acl, 'ACL matches expected value');


  }
}

1;

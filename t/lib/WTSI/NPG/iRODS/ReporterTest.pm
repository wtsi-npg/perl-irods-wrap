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
    my ($self, ) = @_;
    $test_counter++;
    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    my ($self, ) = @_;
    # messaging disabled for test teardown
    my $irods = WTSI::NPG::iRODSMQTest->new(environment          => \%ENV,
                        strict_baton_version => 0,
                        enable_rmq           => 0,
                       );
    $irods->working_collection($cwc);
    $irods->remove_collection($irods_tmp_coll);
}

sub test_message_queue : Test(2) {
    my ($self, ) = @_;
    # ensure the test message queue is working correctly
    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_collection_message($message, $method, $body, $irods);
    $irods->rmq_disconnect();
}

sub test_collection_avu : Test(31) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
        $self->rmq_test_collection_message(
            $messages[$i], $methods[$i], $body, $irods
        );
        $i++;
    }
    $irods->rmq_disconnect();
}

sub test_put_move_collection : Test(21) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    $self->rmq_test_collection_message($messages[0],
                 'put_collection',
                 $put_body,
                 $irods);

    $self->rmq_test_collection_message($messages[1],
                 'move_collection',
                 $moved_body,
                 $irods);

    $irods->rmq_disconnect();
}

sub test_remove_collection : Test(11) {
    my ($self, ) = @_;
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
    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'remove_collection';
    my $body = {avus       => [],
                acl        => \@acl,
        collection => $irods_new_coll,
           };
    $self->rmq_test_collection_message($message, $method, $body, $irods);
    $irods->rmq_disconnect();
}

sub test_set_collection_permissions : Test(21) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    my $method = 'set_collection_permissions';
    $self->rmq_test_collection_message(
        $messages[0], $method, $body_null, $irods
    );
    $self->rmq_test_collection_message(
        $messages[1], $method, $body_own, $irods
    );

    $irods->rmq_disconnect();
}


### data object tests ###

sub test_add_object : Test(12) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_object_message($message, 'add_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_copy_object : Test(12) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_object_message($message, 'copy_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_move_object : Test(12) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_object_message($message, 'move_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_object_avu : Test(34) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
        $self->rmq_test_object_message(
            $messages[$i], $methods[$i], $body, $irods
        );
        $i++;
    }
    $irods->rmq_disconnect();
}

sub test_remove_object : Test(12) {
    my ($self, ) = @_;
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
    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_object_message($message, $method, $body, $irods);
    $irods->rmq_disconnect();
}

sub test_replace_object : Test(12) {
    my ($self, ) = @_;
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

    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_object_message($message, 'replace_object', $body, $irods);
    $irods->rmq_disconnect();
}

sub test_set_object_permissions : Test(23) {
    my ($self, ) = @_;
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
    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
    my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');
    my $method = 'set_object_permissions';

    $self->rmq_test_object_message($messages[0], $method, $body_null, $irods);
    $self->rmq_test_object_message($messages[1], $method, $body_own, $irods);
    $irods->rmq_disconnect();
}

### methods for the Publisher class ###

sub test_publish_object : Test(14) {
    my ($self, ) = @_;
    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       enable_rmq           => 0,
      );
    my $user = 'public';
    my $publisher = WTSI::NPG::iRODS::PublisherWithReporting->new
      (
       irods                => $irods,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    my $published_filename = 'ipsum.txt';
    my $remote_file_path = "$irods_tmp_coll/$published_filename";
    $remote_file_path = $irods->absolute_path($remote_file_path);
    my $pub_obj = $publisher->publish("$data_path/$test_filename",
                      $remote_file_path);
    ok($irods->is_object($remote_file_path), 'File published to iRODS');
    ok($remote_file_path eq $pub_obj->absolute()->str(),
       'Absolute data object paths from input and return value are equal');
    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_object_message($message, $method, $body, $irods);
}

sub test_publish_collection : Test(13) {
    my ($self, ) = @_;
    my $irods = WTSI::NPG::iRODSMQTest->new
      (environment          => \%ENV,
       strict_baton_version => 0,
       enable_rmq           => 0,
      );
    my $user = 'public';
    my $publisher = WTSI::NPG::iRODS::PublisherWithReporting->new
      (
       irods                => $irods,
       routing_key_prefix   => 'test',
       hostname             => $test_host,
       rmq_config_path      => $conf,
       channel              => $test_counter,
      );
    my $pub_coll = $publisher->publish($data_path, $irods_tmp_coll);
    my $dest_coll = $irods_tmp_coll.'/reporter';
    $dest_coll = $irods->absolute_path($dest_coll);
    ok($irods->is_collection($dest_coll), 'Collection published to iRODS');
    ok($dest_coll eq $pub_coll->absolute()->str(),
       'Absolute collection paths from input and return value are equal');
    my $args = $self->rmq_subscriber_args($test_counter, $conf, $test_host);
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
    $self->rmq_test_collection_message($message, $method, $body, $irods);
}

1;

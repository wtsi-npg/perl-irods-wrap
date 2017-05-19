
{
    package WTSI::NPG::RabbitMQ::SimpleSubscriber;

    # simple class to get messages from RabbitMQ for tests

    use Moose;
    use JSON;
    with 'WTSI::NPG::RabbitMQ::Connectable';

    has 'channel' =>
        (is       => 'ro',
         isa      => 'Int',
         default  => 1,
         documentation => 'A RabbitMQ channel',
     );

    sub BUILD {
        my ($self, ) = @_;
        $self->rmq_connect();
        $self->rmq->channel_open($self->channel);
    }

    sub DEMOLISH {
        my ($self, ) = @_;
        $self->rmq_disconnect();
    }

    sub read_next {
        my ($self, $queue_name) = @_;
        my $gotten = $self->rmq->get($self->channel, $queue_name);
        my ($body, $headers);
        my $body_string = $gotten->{body};
        if (defined $body_string) {
            $body = decode_json($body_string);
            $headers = $gotten->{props}->{headers};
        }
        return ($body, $headers);
    }

    sub read_all {
        my ($self, $queue_name) = @_;
        my @messages;
        my ($body, $headers);
        do {
            ($body, $headers) = $self->read_next($queue_name);
            if (defined $body) {
                push @messages, [$body, $headers];
            }
        } while ($body);
        return @messages;
    }


    __PACKAGE__->meta->make_immutable;

    no Moose;

    1;

}


package WTSI::NPG::iRODS::ReporterTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use Log::Log4perl;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::iRODS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $log = Log::Log4perl::get_logger();

use WTSI::NPG::iRODS;

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
my $ssubargs = {
        hostname             => $test_host,
        rmq_config_path      => $conf,
    }; # arguments for SimpleSubscriber creation

### TODO skip tests with a warning message if RabbitMQ server is not found

sub setup_test : Test(setup) {
    # clear the message queue
    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    if (scalar @messages > 0) {
        $log->warn('Got ', scalar @messages,
                   ' unread RMQ messages from previous tests');
    }
    # messaging disabled for test setup
    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      no_rmq               => 1,
                                  );
    $cwc = $irods->working_collection;
    $irods_tmp_coll =
        $irods->add_collection("PublisherTest.$pid.$test_counter");
    $remote_file_path = "$irods_tmp_coll/lorem.txt";
    $irods->add_object("$data_path/lorem.txt", $remote_file_path);
    $test_counter++;
}

sub teardown_test : Test(teardown) {
    # messaging disabled for test teardown
    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
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

sub test_add_collection : Test(13) {
    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );

    my $irods_new_coll = $irods_tmp_coll.'/temp';
    $irods->add_collection($irods_new_coll);
    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'add_collection';
    _test_collection_message($message, $method);
}


sub test_collection_avu : Test(40) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );

    $irods->add_collection_avu($irods_tmp_coll, 'colour', 'green');
    $irods->add_collection_avu($irods_tmp_coll, 'colour', 'purple');
    $irods->remove_collection_avu($irods_tmp_coll, 'colour', 'green');

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 3, 'Got 3 messages from queue');

    my @methods = qw/add_collection_avu
                     add_collection_avu
                     remove_collection_avu/;
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

sub test_put_move_collection : Test(25) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );

    $irods->put_collection($data_path, $irods_tmp_coll);
    my $dest_coll = $irods_tmp_coll.'/reporter';
    my $moved_coll = $irods_tmp_coll.'/reporter.moved';
    $irods->move_collection($dest_coll, $moved_coll);

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    my $i = 0;
    my @methods = qw/put_collection move_collection/;

    foreach my $message (@messages) {
        _test_collection_message($message, $methods[$i]);
        $i++;
    }
}

sub test_remove_collection : Test(13) {
    my $irods_no_rmq = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                             strict_baton_version => 0,
                                             no_rmq               => 1,
                                         );
    my $irods_new_coll = $irods_tmp_coll.'/temp';
    $irods_no_rmq->add_collection($irods_new_coll);

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );
    $irods->remove_collection($irods_new_coll);
    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'remove_collection';
    _test_collection_message($message, $method);
}

sub test_set_collection_permissions : Test(25) {
    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
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

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');

    my $method = 'set_collection_permissions';

    foreach my $message (@messages) {
        _test_collection_message($message, $method);
    }
}


### data object tests ###

sub test_add_object : Test(49) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );
    my $added_remote_path = "$irods_tmp_coll/lorem_copy.txt";
    $irods->add_object("$data_path/lorem.txt", $added_remote_path);

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 3, 'Got 3 messages from queue');

    my @methods = qw[add_object_avu remove_object_avu add_object];
    my $i = 0;

    foreach my $message (@messages) {
        _test_object_message($message, $methods[$i]);
        $i++;
        my ($body, $headers) = @{$message};
        # temporary staging object is named lorem_copy.txt.[suffix]
        ok($body->{'data_object'} =~ /^lorem_copy\.txt/msx,
           'Data object name starts with lorem_copy.txt');
        ok($body->{'collection'} eq $irods_tmp_coll,
           "Collection name is $irods_tmp_coll");
    }
}

sub test_copy_object : Test(17) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );
    my $copied_remote_path = "$irods_tmp_coll/lorem_copy.txt";
    $irods->copy_object($remote_file_path, $copied_remote_path);

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $method = 'copy_object';
    my $message = shift @messages;
    _test_object_message($message, $method);

    my ($body, $headers) = @{$message};
    ok($body->{'data_object'} eq 'lorem_copy.txt',
       'Data object name is lorem_copy.txt');
    ok($body->{'collection'} eq $irods_tmp_coll,
       "Collection name is $irods_tmp_coll");
}

sub test_move_object : Test(17) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );
    my $moved_remote_path = "$irods_tmp_coll/lorem_moved.txt";
    $irods->move_object($remote_file_path, $moved_remote_path);

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 1, 'Got 1 message from queue');

    my $method = 'move_object';
    my $message = shift @messages;
    _test_object_message($message, $method);

    my ($body, $headers) = @{$message};
    ok($body->{'data_object'} eq 'lorem_moved.txt',
       'Data object name is lorem_moved.txt');
    ok($body->{'collection'} eq $irods_tmp_coll,
       "Collection name is $irods_tmp_coll");
}

sub test_object_avu : Test(52) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );

    $irods->add_object_avu($remote_file_path, 'colour', 'green');
    $irods->add_object_avu($remote_file_path, 'colour', 'purple');
    $irods->remove_object_avu($remote_file_path, 'colour', 'green');

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 3, 'Got 3 messages from queue');

    my @methods = qw/add_object_avu add_object_avu remove_object_avu/;
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

sub test_remove_object : Test(15) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );
    $irods->remove_object($remote_file_path);
    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 1, 'Got 1 message from queue');

    my $message = shift @messages;
    my $method = 'remove_object';
    _test_object_message($message, $method);
}

sub test_replace_object : Test(65) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
                                  );
    $irods->replace_object("$data_path/lorem.txt", $remote_file_path);

    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);

    is(scalar @messages, 4, 'Got 4 messages from queue');

    my @methods = qw[add_object_avu
                     remove_object
                     remove_object_avu
                     replace_object];
    my $i = 0;

    foreach my $message (@messages) {
        _test_object_message($message, $methods[$i]);
        $i++;
        my ($body, $headers) = @{$message};
        # temporary staging object is named lorem.txt.[suffix]
        ok($body->{'data_object'} =~ /^lorem\.txt/msx,
           'Data object name starts with lorem.txt');
        ok($body->{'collection'} eq $irods_tmp_coll,
           "Collection name is $irods_tmp_coll");
    }
}

sub test_set_object_permissions : Test(29) {
    # change permissions on a data object, with messaging
    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0,
                                      routing_key_prefix   => 'test',
                                      hostname             => $test_host,
                                      rmq_config_path      => $conf,
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
    my $subscriber = WTSI::NPG::RabbitMQ::SimpleSubscriber->new($ssubargs);
    my @messages = $subscriber->read_all($queue);
    is(scalar @messages, 2, 'Got 2 messages from queue');
    my $method = 'set_object_permissions';

    foreach my $message (@messages) {
        _test_object_message($message, $method);
    }
}

### methods for repeated tests ###

sub _test_collection_message {
    my ($message, $method) = @_;
    my ($body, $headers) = @{$message};
    my @body_keys_coll = qw[collection
                            timestamps
                            access
                            avus];
    my $expected_body_coll = scalar @body_keys_coll;
    ok(scalar keys(%{$headers}) == $expected_headers,
       'Found '.$expected_headers.' header key/value pairs.');
    ok(scalar keys(%{$body}) == $expected_body_coll,
       'Found '.$expected_body_coll.' body key/value pairs.');
    ok($headers->{'method'} eq $method, 'Method name is '.$method);

    foreach my $key (@header_keys) {
        ok(defined $headers->{$key},
           'Value defined in message header for '.$key);
    }
    foreach my $key (@body_keys_coll) {
        ok(defined $body->{$key},
           'Value defined in message body for '.$key);
    }
}

sub _test_object_message {
    my ($message, $method) = @_;
    my ($body, $headers) = @{$message};
    my @body_keys_obj = qw[collection
                           data_object
                           timestamps
                           access
                           avus
                           replicates];
    my $expected_body_obj = scalar @body_keys_obj;
    ok(scalar keys(%{$headers}) == $expected_headers,
       'Found '.$expected_headers.' header key/value pairs.');
    ok(scalar keys(%{$body}) == $expected_body_obj,
       'Found '.$expected_body_obj.' body key/value pairs.');
    ok($headers->{'method'} eq $method, 'Method name is '.$method);
    foreach my $key (@header_keys) {
        ok(defined $headers->{$key},
           'Value defined in message header for '.$key);
    }
    foreach my $key (@body_keys_obj) {
        ok(defined $body->{$key},
           'Value defined in message body for '.$key);
    }
}

1;

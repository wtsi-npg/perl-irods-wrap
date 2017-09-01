package WTSI::NPG::RabbitMQ::TestCommunicator;

# simple class to send/receive RabbitMQ messages for tests

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

sub publish {
    my ($self, $body, $exchange, $routing_key) = @_;
    $exchange ||= 'npg.gateway';
    $routing_key ||= 'test.irods.rmq_publish';
    my $options = { exchange => $exchange };
    my $time = localtime;
    my $header = { time => $time };
    my $props = { headers => $header };
    # Net::AMQP::RabbitMQ documentation specifies 'header' as key in
    # props argument to 'publish', but this is incorrect (2017-07-31)
    $self->rmq->publish($self->channel,
                        $routing_key,
                        $body,
                        $options,
                        $props);
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

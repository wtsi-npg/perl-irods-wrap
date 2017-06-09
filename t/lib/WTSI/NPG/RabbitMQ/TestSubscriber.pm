package WTSI::NPG::RabbitMQ::TestSubscriber;

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

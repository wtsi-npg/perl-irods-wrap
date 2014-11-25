
package WTSI::NPG::iRODS::MetaSearcher;

use File::Spec;
use Moose;

extends 'WTSI::NPG::iRODS::Communicator';

has '+executable' => (default => 'baton-metaquery');

around [qw(search)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::MetaSearcher ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub search {
  my ($self, $zone_hint, @avus) = @_;

   defined $zone_hint or
     $self->logconfess('A defined zone_hint argument is required');

  my $i = 0;
  foreach my $avu (@avus) {
    unless (ref $avu eq 'HASH') {
      $self->logconfess('A query AVU must be a HashRef: AVU #$i was not');
    }
    unless ($avu->{attribute}) {
      $self->logconfess('A query AVU must have an attribute: AVU #$i did not');
    }
    unless ($avu->{value}) {
      $self->logconfess('A query AVU must have a value: AVU #$i did not');
    }
    $i++;
  }

  my $spec = {collection => $zone_hint,
              avus       => \@avus};

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  my @results =  map { $self->path_spec_str($_) } @$response;

  return \@results;
}

sub validate_response {
  my ($self, $response) = @_;

  # The ony valid response is a HashRef or ArrayRef
  my $rtype = ref $response;
  unless ($rtype eq 'HASH' || $rtype eq 'ARRAY') {
    $self->logconfess("Failed to get a HashRef or Array response; got $rtype");
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

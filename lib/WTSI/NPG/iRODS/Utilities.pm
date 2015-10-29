package WTSI::NPG::iRODS::Utilities;

use strict;
use warnings;
use List::AllUtils qw(uniq);
use Moose::Role;

use WTSI::DNAP::Utilities::Runnable;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '';

our $MD5SUM = 'md5sum';

=head2 hash_path

  Arg [1]    : String path to a file.
  Arg [2]    : MD5 checksum (optional).

  Example    : my $path = $irods->hash_path($filename)
  Description: Return a hashed path 3 directories deep, each level having
               a maximum of 256 subdirectories, calculated from the file's
               MD5. If the optional MD5 argument is supplied, the MD5
               calculation is skipped and the provided value is used instead.
  Returntype : Str

=cut

sub hash_path {
  my ($self, $file, $md5sum) = @_;

  $md5sum ||= $self->md5sum($file);
  unless ($md5sum) {
    $self->logconfess("Failed to caculate an MD5 for $file");
  }

  my @levels = $md5sum =~ m{\G(..)}gmsx;

  return (join q{/}, @levels[0..2]);
}

=head2 md5sum

  Arg [1]    : String path to a file.

  Example    : my $md5 = $irods->md5sum($filename)
  Description: Calculate the MD5 checksum of a local file.
  Returntype : Str

=cut

sub md5sum {
  my ($self, $file) = @_;

  defined $file or $self->logconfess('A defined file argument is required');
  $file eq q{} and $self->logconfess('A non-empty file argument is required');

  my @result = WTSI::DNAP::Utilities::Runnable->new
    (executable  => $MD5SUM,
     arguments   => [$file],
     environment => $self->environment,
     logger      => $self->logger)->run->split_stdout;
  my $raw = shift @result;

  my ($md5) = $raw =~ m{^(\S+)\s+.*}msx;

  return $md5;
}

=head2 make_avu

  Arg [1]    : An attribute, Str
  Arg [2]    : A value, Str
  Arg [3]    : Units, Str or undef. (Optional, defaults to undef)

  Example    : my $avu = $irods->make_avu($attr, $value);
  Description: Return a new AVU of the form
                 {attribute => $attribute,
                  value     => $value,
                  units     => $units}
               The units key is absent if units are not defined.
  Returntype : HashRef

=cut

sub make_avu {
  my ($self, $attribute, $value, $units) = @_;

  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq q{} and
    $self->logconfess('A non-empty value argument is required');

  my $avu = {attribute => $attribute,
             value     => $value};
  if (defined $units) {
    $avu->{units} = $units;
  }

  return $avu;
}

=head2 avus_equal

  Arg [1]    : An AVU, HashRef.
  Arg [2]    : An AVU, HashRef.

  Example    : $self->avus_equal({attribute => $a1,
                                  value     => $v1},
                                 {attribute => $a2,
                                  value     => $v2});
  Description: Return true if two AVUs are equal (they share the same
               attribute and value strings (and units, if present).
  Returntype : Bool

=cut

sub avus_equal {
  my ($self, $avu_x, $avu_y) = @_;

  defined $avu_x or $self->logconfess('A defined avu_x argument is required');
  defined $avu_y or $self->logconfess('A defined avu_y argument is required');

  ref $avu_x eq 'HASH' or
    $self->logconfess('The avu_x argument must be a HashRef');
  ref $avu_y eq 'HASH' or
    $self->logconfess('The avu_y argument mus be a HashRef');

  my $false = 0;
  return ((defined $avu_x->{units} && defined $avu_y->{units} &&
           $avu_x->{attribute} eq $avu_y->{attribute} &&
           $avu_x->{value}     eq $avu_y->{value} &&
           $avu_x->{units}     eq $avu_y->{units})
          ||
          (!defined $avu_x->{units} && !defined $avu_y->{units} &&
           $avu_x->{attribute} eq $avu_y->{attribute} &&
           $avu_x->{value}     eq $avu_y->{value})
          ||
          $false);
}

=head2 avu_str

  Arg [1]    : An AVU, HashRef.

  Example    : $self->avu_str({attribute => $a1,
                               value     => $v1,
                               units     => undef});
  Description: Return a stringified representation of the AVU.
  Returntype : Str

=cut

sub avu_str {
  my ($self, $avu) = @_;

  defined $avu or $self->logconfess('A defined avu argument is required');

  ref $avu eq 'HASH' or
    $self->logconfess('The avu argument mus be a HashRef');

  my ($attribute, $value, $units) = map { defined $_ ? $_ : 'undef' }
    ($avu->{attribute}, $avu->{value}, $avu->{units});

  return sprintf "{'%s', '%s', '%s'}", $attribute, $value, $units;
}

=head2 remove_duplicate_avus

  Arg [1]    : Array of AVUs, Array[HashRef].

  Example    : my @unique = $irods->remove_duplicate_avus
                  ({attribute => $attribute1,
                    value     => $value1},
                   {attribute => $attribute2,
                    value     => $value2,
                    units     => $units2});

  Description: Return a new Array of AVUs, without duplicates.
  Returntype : Array[HashRef]

=cut

sub remove_duplicate_avus {
  my ($self, @avus) = @_;

  my %metadata_tree;
  foreach my $avu (@avus) {
    my $a = $avu->{attribute};
    my $u = $avu->{units} || q{}; # Empty string as a hash key proxy
                                  # for undef

    if (exists $metadata_tree{$a}{$u}) {
      push @{$metadata_tree{$a}{$u}}, $avu->{value}
    }
    else {
      $metadata_tree{$a}{$u} = [$avu->{value}]
    }
  }

  my @uniq;
  foreach my $a (keys %metadata_tree) {
    foreach my $u (keys $metadata_tree{$a}) {
      my @values = uniq @{$metadata_tree{$a}{$u}};

      foreach my $v (@values) {
        push @uniq, $self->make_avu($a, $v, $u ? $u : undef);
      }
    }
  }

  return $self->sort_avus(@uniq);
}

=head2 sort_avus

  Arg [1]    : Array of AVUs, Array[HashRef].

  Example    : my @sortef = $irods->sort_avus
                  ({attribute => $attribute1,
                    value     => $value1},
                   {attribute => $attribute2,
                    value     => $value2,
                    units     => $units2});

  Description: Return a new Array of AVUs, sorted first by attribute,
               then by value, then by units.
  Returntype : Array[HashRef]

=cut

sub sort_avus {
  my ($self, @avus) = @_;

  my @sorted = sort {
     $a->{attribute} cmp $b->{attribute}                    ||
     $a->{value}     cmp $b->{value}                        ||
     (( defined $a->{units} && !defined $b->{units}) && -1) ||
     ((!defined $a->{units} &&  defined $b->{units}) &&  1) ||
     $a->{units}     cmp $b->{units} } @avus;

  return @sorted;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Utilities

=head1 DESCRIPTION

Provides utility methods that have been factored out of other
packages.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut

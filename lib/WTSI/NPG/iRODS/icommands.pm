package WTSI::NPG::iRODS::icommands;

use strict;
use warnings;

use English qw(-no_match_vars);
use Exporter qw[import];
our @EXPORT_OK = qw[iquest];

our $VERSION = '';

my $log = Log::Log4perl->get_logger(__PACKAGE__);

=head2 iquest

  Arg [1]    : iquest arguments

  Example    : my @result = iquest('-z', $zone, q[%s],
                              "select COLL_NAME where COLL_NAME like '$coll%'");
  Description: Return results of an iquest query. The arguments passed to iquest
               should be strings as used on the command line. The exception to
               this is that the '--no-page' argument may be omitted as this
               function always fetches all results without paging. It is the
               caller's responsibility to ensure correct quoting within the
               query string. If a query finds no results, an empty array is
               returned.
  Returntype : Array of result strings

=cut
sub iquest {
  my (@args) = @_;

  @args = grep { $_ ne '--no-page' } @args;

  my @iquest_cmd = ('iquest', '--no-page', @args);
  my $cmd = join q[ ], @iquest_cmd;

  $log->debug("Executing '$cmd'");
  open my $fh, q[-|], @iquest_cmd or
    $log->logcroak("Failed to open pipe from '$cmd': $ERRNO");

  my @records;
  while (my $line = <$fh>) {
    chomp $line;
    next if $line =~ m{^\s*$}msx;

    # Work around the iquest bug/misfeature where it mixes its logging output
    # with its data output
    next if $line =~ m{^Zone is}msx;
    next if $line =~ m{^CAT_NO_ROWS_FOUND}msx;
    # Skip record separators. These are not printed consistently; when there
    # are sufficient records to cause pagination, the separator is missing.
    next if $line =~ m{^----$}msx;

    $log->debug("iquest: $line");
    push @records, $line;
  }

  my $rc = close $fh;
  my $exit_code = $CHILD_ERROR >> 8;
  if ($exit_code == 0) {
    return @records;
  }
  if ($exit_code == 1) {
    $log->debug("Empty result from '$cmd'");
    return @records;
  }
  if ($rc) {
    $log->logcroak("Failed close STDOUT of iquest '$cmd': $exit_code");
  }

  $log->logcroak("Failed to exit iquest cleanly '$cmd': $exit_code");

  return;
}

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::icommands

=head1 DESCRIPTION

Wrapper for the following iRODS icommands:

    iquest

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2023 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut

#!/usr/bin/env perl

use warnings;
use strict;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;
use Try::Tiny;

use WTSI::NPG::iRODS;

our $VERSION = '';

my $debug;
my $dest;
my $verbose;
my $source;

GetOptions('debug'         => \$debug,
           'destination=s' => \$dest,
           'help'          => sub {pod2usage(-verbose => 2,
                                             -exitval => 0)},
           'verbose'       => \$verbose,
           'source=s'      => \$source);


# Unless verbose, turn the logging off to avoid reporting whole stacktraces
my $level = $debug ? $DEBUG : $verbose ? $INFO : $OFF;
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $level,
                          utf8   => 1 });

if (not $source) {
  my $msg = 'A --source-collection argument is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

if (not $dest) {
  my $msg = 'A --dest-collection argument is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

try {
  WTSI::NPG::iRODS->new->move_collection($source, $dest);
}
catch {
  my @stack = split /\n/msx; # Chop up the stack trace
  print STDERR shift @stack, "\n";
  exit 1;
};

__END__

=head1 NAME

npg_move_collection

=head1 SYNOPSIS

npg_move_collection --source <path> --destination <path>
  [--debug] [--verbose]

 Options:
   --source        The source collection in iRODS.
   --debug         Enable debug level logging. Optional, defaults to false.
   --destination   The destination collection in iRODS.
   --verbose       Print messages while processing. Optional.

=head1 DESCRIPTION

Move an iRODS collection recursively, including all contents and metadata.

Copies all sub-collections and data objects with their respective metadata.
This script is idempotent within the guarantees provided by iRODS. If
interrupted, it may be run again with the same arguments to complete the
operation.

=head1 AUTHOR

Keith James kdj@sanger.ac.uk

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2021 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut

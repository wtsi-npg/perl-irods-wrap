package WTSI::NPG::TestMQiRODS;

use Moose;

use WTSI::NPG::iRODS;

extends 'WTSI::NPG::iRODS';
with 'WTSI::NPG::iRODS::Reportable';

__PACKAGE__->meta->make_immutable;

no Moose;

1;

use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'SAPO::Broker',
    AUTHOR              => 'Delfim Machado <dbcm@co.sapo.pt>',
    VERSION_FROM        => 'lib/SAPO/Broker.pm',
    ABSTRACT_FROM       => 'lib/SAPO/Broker.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
	'XML::LibXML' => 0,
	'XML::LibXML::XPathContext' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'SAPO-Broker-*' },
);

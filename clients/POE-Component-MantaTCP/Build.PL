use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'POE::Component::MantaTCP',
    license             => 'perl',
    dist_author         => 'Pedro Melo <melo@co.sapo.pt>',
    dist_version_from   => 'lib/POE/Component/MantaTCP.pm',
    script_files       => ['bin/sapo-broker-sniffer', 'bin/sapo-broker-publisher'],
    
    requires => {
        'Test::More' => 0,
        'version'    => 0,
        'POE'        => 0,
        'POE::Component::Client::TCP'    => 0,
        'POE::Filter::Block' => 0,
        'XML::LibXML' => 0,
        'XML::LibXML::XPathContext' => 0,
    },

    add_to_cleanup      => [ 'POE-Component-MantaTCP-*' ],
);

$builder->create_build_script();

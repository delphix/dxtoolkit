# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copyright (c) 2015,2016 by Delphix. All rights reserved.
#
# Program Name : Template_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - Template_obj - class which map a Delphix Engine template API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#



package Template_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);

# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering Template_obj::constructor",1);

    my %templates;
    my $self = {
        _templates => \%templates,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadTemplateList($debug);
    return $self;
}


# Procedure getTemplateByName
# parameters: 
# - name 
# Return template reference for particular name

sub getTemplateByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Template_obj::getTemplateByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $templateitem ( sort ( keys %{$self->{_templates}} ) ) {

        if ( $self->getName($templateitem) eq $name) {
            $ret = $templateitem; 
        }
    }

    return $ret;
}

# Procedure getTemplate
# parameters: 
# - reference
# Return template hash for specific template reference

sub getTemplate {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Template_obj::getTemplate",1);    

    my $templates = $self->{_templates};
    return $templates->{$reference};
}


# Procedure getTemplateParameters
# parameters: 
# - reference
# Return template parmeters for specific template reference

sub getTemplateParameters {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Template_obj::getTemplateParameters",1);    

    my $templates = $self->{_templates};
    return $templates->{$reference}->{parameters};
}

# Procedure compare
# parameters: 
# - reference
# - hash with init.ora
# Return three sets - array existing in template but not in VDB and 
# array existing in VDB but not in template, hash differences

sub compare {
    my $self = shift;
    my $reference = shift;
    my $init = shift;
    
    logger($self->{_debug}, "Entering Template_obj::compare",1);    

    my %restricted = (
      active_instance_count => 1,
      cluster_database => 1,
      cluster_database_instances => 1,
      cluster_interconnects => 1,
      control_files => 1,
      db_block_size => 1,
      db_create_file_dest => 1,
      db_create_online_log_dest_1 => 1,
      db_create_online_log_dest_2 => 1,
      db_create_online_log_dest_3 => 1,
      db_create_online_log_dest_4 => 1,
      db_create_online_log_dest_5 => 1,
      db_file_name_convert => 1,
      db_name => 1,
      db_recovery_file_dest => 1,
      db_recovery_file_dest_size => 1,
      db_unique_name => 1,
      dg_broker_config_file1 => 1,
      dg_broker_config_file2 => 1,
      dg_broker_start => 1,
      fal_client => 1,
      fal_server => 1,
      instance_name => 1,
      instance_number => 1,
      local_listener => 1,
      log_archive_config => 1,
      log_archive_dest => 1,
      log_archive_duplex_dest => 1,
      log_file_name_convert => 1,
      spfile => 1,
      standby_archive_dest => 1,
      standby_file_management => 1,
      thread => 1,
      undo_tablespace => 1,
      __db_cache_size => 1,
      __java_pool_size => 1,
      __large_pool_size => 1,
      __oracle_base => 1,
      __pga_aggregate => 1,
      __pga_aggregate_target => 1,
      __data_transfer_cache_size => 1,
      __sga_target => 1,
      __shared_io_pool_size => 1,
      __shared_pool_size => 1,
      __streams_pool_size => 1,
      _omf => 1
    );
    
    my %settodefault = (
      filesystemio_options => 1,
      audit_file_dest => 1,
      audit_sys_operations => 1,
      audit_trail => 1,
      background_dump_dest => 1,
      core_dump_dest => 1,
      db_domain => 1,
      diagnostic_dest => 1,
      dispatchers => 1,
      fast_start_mttr_target => 1,
      log_archive_dest_1 => 1,
      log_archive_dest_2 => 1,
      log_archive_dest_3 => 1,
      log_archive_dest_4 => 1,
      log_archive_dest_5 => 1,
      log_archive_dest_6 => 1,
      log_archive_dest_7 => 1,
      log_archive_dest_8 => 1,
      log_archive_dest_9 => 1, 
      log_archive_dest_10 => 1,
      log_archive_dest_11 => 1,
      log_archive_dest_12 => 1,
      log_archive_dest_13 => 1,
      log_archive_dest_14 => 1,
      log_archive_dest_15 => 1,
      log_archive_dest_16 => 1,
      log_archive_dest_17 => 1,
      log_archive_dest_18 => 1,
      log_archive_dest_19 => 1,
      log_archive_dest_20 => 1,
      log_archive_dest_21 => 1,
      log_archive_dest_22 => 1,
      log_archive_dest_23 => 1,
      log_archive_dest_24 => 1,
      log_archive_dest_25 => 1,
      log_archive_dest_26 => 1,
      log_archive_dest_27 => 1,
      log_archive_dest_28 => 1,
      log_archive_dest_29 => 1,
      log_archive_dest_30 => 1,
      log_archive_dest_31 => 1,
      log_archive_dest_state_1 => 1,
      log_archive_dest_state_2 => 1,
      log_archive_dest_state_3 => 1,
      log_archive_dest_state_4 => 1,
      log_archive_dest_state_5 => 1,
      log_archive_dest_state_6 => 1,
      log_archive_dest_state_7 => 1,
      log_archive_dest_state_8 => 1,
      log_archive_dest_state_9 => 1,
      log_archive_dest_state_10 => 1,
      log_archive_dest_state_11 => 1,
      log_archive_dest_state_12 => 1,
      log_archive_dest_state_13 => 1,
      log_archive_dest_state_14 => 1,
      log_archive_dest_state_15 => 1,
      log_archive_dest_state_16 => 1,
      log_archive_dest_state_17 => 1,
      log_archive_dest_state_18 => 1,
      log_archive_dest_state_19 => 1,
      log_archive_dest_state_20 => 1,
      log_archive_dest_state_21 => 1,
      log_archive_dest_state_22 => 1,
      log_archive_dest_state_23 => 1,
      log_archive_dest_state_24 => 1,
      log_archive_dest_state_25 => 1,
      log_archive_dest_state_26 => 1,
      log_archive_dest_state_27 => 1,
      log_archive_dest_state_28 => 1,
      log_archive_dest_state_29 => 1,
      log_archive_dest_state_30 => 1,
      log_archive_dest_state_31 => 1,
      remote_listener => 1,
      user_dump_dest => 1,
    );
    
    my %notintemplate;
    my %notininit;
    my %different;
    
    my $templatepar = $self->getTemplateParameters($reference);
    
    for my $par (sort keys %{$templatepar}) {
      if (defined($init->{$par})) {
        $init->{$par} =~ s/['|"]//g;
        $templatepar->{$par} =~ s/['|"]//g;
        if ($init->{$par} ne $templatepar->{$par}) {
          $different{$par} = {
            'init' => $init->{$par},
            'template' => $templatepar->{$par}
          };
        }
      } else {
        $notininit{$par} = $templatepar->{$par};
      }
      
    }
    
    for my $par (sort keys %{$init}) {
      if (!defined($restricted{$par})) {
        if (!defined($templatepar->{$par})) {
          if (!defined($settodefault{$par})) {
            $notintemplate{$par} = $init->{$par};
          }
        }
      }
      
    }
    
    return (\%notininit, \%notintemplate, \%different);
    
    
  }


# Procedure getTemplateList
# parameters: 
# Return template list

sub getTemplateList {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Template_obj::getTemplateList",1);    

    return keys %{$self->{_templates}};
}


# Procedure getName
# parameters: 
# - reference
# Return template name for specific template reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Template_obj::getName",1);   

    my $templates = $self->{_templates};
    return $templates->{$reference}->{name};
}

# Procedure exportTemplate
# parameters: 
# - reference
# - location - directory
# Return 0 if no errors

sub exportTemplate {
    my $self = shift;
    my $reference = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Template_obj::exportTemplate",1);   

    my $filename =  $location . "/" . $self->getName($reference) . ".template";

    my $templates = $self->{_templates};

    open (my $FD, '>', "$filename") or die ("Can't open file $filename : $!");

    print "Exporting template into file $filename \n";

    print $FD to_json($templates->{$reference}, {pretty => 1});

    close $FD;

    return 0;
}

# Procedure importTemplate
# parameters: 
# - location - file name
# Return 0 if no errors

sub importTemplate {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Template_obj::importTemplate",1);   

    my $filename =  $location;

    my $loadedTemplate;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedTemplate = $json->decode(<$FD>);
    
    close $FD;



    delete $loadedTemplate->{reference};
    delete $loadedTemplate->{namespace};

    $self->loadTemplateList();

    if (defined($self->getTemplateByName($loadedTemplate->{name}))) {
        print "Template " . $loadedTemplate->{name} . " from file $filename already exist.\n";
        return 0;
    }

    print "Importing template from file $filename.";

    my $json_data = to_json($loadedTemplate);

    my $operation = 'resources/json/delphix/database/template';

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);  

    if ($result->{status} eq 'OK') {
        print " Import completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure updateTemplate
# parameters: 
# - location - file name
# Return 0 if no errors

sub updateTemplate {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Template_obj::updateTemplate",1);   

    my $filename =  $location;

    my $loadedTemplate;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedTemplate = $json->decode(<$FD>);
    
    close $FD;

    delete $loadedTemplate->{reference};
    delete $loadedTemplate->{namespace};



    $self->loadTemplateList();

    if (! defined($self->getTemplateByName($loadedTemplate->{name}))) {
        print "Template " . $loadedTemplate->{name} . " from file $filename doesn't exist. Can't update.\n";
        return 1;
    } 

    my $reference = $self->getTemplateByName($loadedTemplate->{name});

    print "Updating template " . $loadedTemplate->{name} . " from file $filename.";

    my $json_data = to_json($loadedTemplate);

    my $operation = 'resources/json/delphix/database/template/' . $reference;

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);  

    if ($result->{status} eq 'OK') {
        print " Update completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure loadTemplateList
# parameters: none
# Load a list of template objects from Delphix Engine

sub loadTemplateList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Template_obj::loadTemplateList",1);   

    my $operation = "resources/json/delphix/database/template";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my $templates = $self->{_templates};

        for my $templateitem (@res) {
            $templates->{$templateitem->{reference}} = $templateitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;
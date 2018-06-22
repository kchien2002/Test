#!/usr/bin/perl
#
# Date: 2018/06/19
# By Kilion Chien
# Description:
# This script will query the database and do the following:
# 1. Fetch the temp column.
# 2. Display the average.
###############################################################
#
# Main Program
#
###############################################################
use DBI;
use DBD::mysql;
use warnings;
use strict;

my @data;

#MySQL connect details
my $db = "test";
my $host = "localhost";
my $port = "3306";
my $user = "user";
my $password = "123456";
my $dsn = "DBI:mysql:database=$db;host=$host;port=$port";
my $dbh = DBI->connect($dsn, $user, $password, {'RaiseError' => 1})or die "Unable to connect: $DBI::errstr\n";
print "Opened database successfully!\n";

#Query the database
my $sql = qq{ SELECT temp FROM tabledata };
my $sth = $dbh->prepare($sql);
$sth->execute() or die "SQL Error: $DBI::errstr\n";

#Process the data
#Use Bind Columns for better performance
my ($a);
$sth->bind_columns(undef, \$a);
while ($sth->fetch)
{
 push(@data, $a);
}
print ("Fetched data from 'temp'.\n");

#Disconnect from database
$dbh->disconnect();
print "Disconnected from Database.\n";

#Call sub average to calculate the average value
my $avg = average(@data); 
print ("The values in 'temp' are: @data\n");
print ("The average is: $avg\n");

#Sub routines
sub average {
my @array = @_;
my $sum;
foreach (@array) { $sum += $_; }
return $sum/@array;
}
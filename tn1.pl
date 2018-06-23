#!/usr/bin/perl
##
## Date: 2018/06/19
## By Kilion Chien
## Description:
## This script will read a text file and do the following:
## 1. Parse data.
## 2. Create and insert the csv data into a table in a mysql database.
###############################################################
#
# Main Program
#
###############################################################
use DBI;
use DBD::mysql;
use warnings;
use strict;

my @header;
my @data;
my @name;

#Check if the script is used on a file.
if (@ARGV < 1)
{
 print ("Usage: task1.pl <datafile>\n\n");
 exit;
}
my $fileName = $ARGV[0];

#Read the file.
open (FILE, "$fileName") or die "$!\n";
while(defined(my $line = <FILE>))
{
 chomp($line);
 next if ($line eq "");
#Replace white character with space
 $line =~ s/\s+/ /g;
#Take the first row as the columns for database
 if ($. == 1)
 {
  @header = split(/\,/, $line);
 }
#Take the rest rows as the data for database
 else
 {
  push(@data, split(/\,/, $line));
 }
}
close (FILE);
#For debug
#print ("@header\n");
#print ("@data\n");

#$Check if the column 'name' exists
if (grep(/^name$/o, @header) == 0)
{
#Post error that the column 'name' doesn't exist.
 print ("Error: The column 'name' doesn't exist in $fileName, .\n");
}
#Connect to database only if the column 'name' exists
else
{
 ### MySQL connect details
 my $db = "test";
 my $host = "localhost";
 my $port = "3306";
 my $user = "user";
 my $password = "123456";
 my $dsn = "DBI:mysql:database=$db;host=$host;port=$port";
 #Connect to database
 my $dbh = DBI->connect($dsn, $user, $password, {'RaiseError' => 1})or die "Unable to connect: $DBI::errstr\n";
 print "Opened database successfully!\n";
 
 #Create a new table 'tabledata' with a default column 'name' if it doesn't exist
 my $create_table = $dbh->prepare("CREATE TABLE IF NOT EXISTS tabledata (name text);");
 $create_table->execute() or die "SQL Error: $DBI::errstr\n";
 print ("The table 'testdata' and column 'name' are ready in the database.\n");
 
 #Create the new column parsed from the file into the table if it doesn't exist
 for (my $c=1; $c <= @header; $c++)
 {
 #Check if the column already exists
  my $check_column = $dbh->do("SHOW COLUMNS FROM tabledata LIKE '$header[$c-1]';");
 #Add the column if it doesn't exist
  if($check_column != 1)
  {
   $dbh->do("ALTER TABLE tabledata ADD $header[$c-1] varchar(50);");
   print ("Added a new column: $header[$c-1]\n");
  }
 #First only insert the names parsed from the file into the name column
 #Use Placeholders and bind_param for better performance
  my $s = qq{  INSERT INTO tabledata(name) VALUES(?) };
  my $sth = $dbh->prepare($s);
  if ($header[$c-1] eq 'name')
  {
 #Store the position of the name column in the header array
   my $name_id = $c-1;
   for (my $d=1; $d <= @data; $d++)
   {
 #Use the position of name column to calculate the position of name records in the data array.
    my $insert=$data[$name_id+($d-1)*@header];
    if ($insert)
    {
 #Store the names so that the other data cab be updated based on that
     push(@name, $insert);
 #Insert the names into database
     $sth->bind_param(1, $insert);
     $sth->execute() or die "SQL Error: $DBI::errstr\n";
 #For debug
 #print "$insert\n";
    }
   }
  }
 }
 
 #Update the other columns parsed from the file associated with the inserted names
 for (my $c=0; $c <= (@header-1); $c++)
 {
  if ($header[$c] ne 'name')
  {
   for (my $d=0; $d <= (@name-1); $d++)
   {
 #Calculate the positions of column in the header array and data in the data array
    my $column=$header[$c];
    my $update=$data[$d*@header+$c];
 #Update the data on the right record in database
    $dbh->do("UPDATE tabledata SET $column = '$update' where name = '$name[$d]' AND ISNULL($column)");
 #For debug
 #print "$name[$d]\n";
 #print "$column\n";
 #print "$update\n";
   }
  }
 }
 print "Updated data.\n";
 #Disconnect from database
 $dbh->disconnect();
 print "Disconnected from Database.\n";
}

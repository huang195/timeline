#!/bin/sh

if [ -z $1 ]
then
	echo "usage: ./rpm2conf.sh <rpm filename>"
	exit 1
fi

packagename=`echo $1 | sed 's/\.rpm$//' | sed 's/.*\///'`
mkdir -p conf

rpm -q -p $1 --dump | \
awk '\
BEGIN{printf "[\n"} \
{printf "\t{\"name_s\": \"%s\", \"size_i\": \"%d\"},\n", $1, $2} \
END{printf "]\n"}' > conf/$packagename.lst

# remove comma in the second to last line to make it json compatible
sed -i ':a;N;$!ba;s/},\n\]/}\n\]/g' conf/$packagename.lst

echo "{" > conf/$packagename.conf
echo "	\"pattern\":	[\"@$packagename.lst\"]," >> conf/$packagename.conf
echo "	\"tag\":	\"$packagename\"," >> conf/$packagename.conf
echo "	\"cat\":	\"package\"" >> conf/$packagename.conf
echo "}" >> conf/$packagename.conf



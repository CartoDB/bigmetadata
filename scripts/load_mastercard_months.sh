#!/usr/bin/env bash

files=`ls *.dump`

for file in ${files[@]}; do
  file_without_extension=`echo $file | sed -e 's/\..*$//'`
  IFS='_' read -ra fragments <<< "$file_without_extension"
  country=${fragments[1]}
  area=`echo $file_without_extension | sed -e "s/mc_${country}_\(.*\)/\1/"`
  cat $file | psql -U postgres -d gis -Atc "copy \"$country.mastercard\".mc_$area from stdin"
done


#!/usr/bin/env bash

# Script to dump all mc tables for given months to current directory.
# Example usage: <PATH_TO_BIGMETADATA>/scripts/dump_mastercard_months.sh "us" "'09/01/2018','10/01/2018'"

country=$1 # Example: 'ca'
months=$2 # Example: "'07/01/2018','08/01/2018','09/01/2018','10/01/2018'"

AULevels=('mesh_block' 'sa1' 'sa2' 'sa3' 'sa4' 'state')
CALevels=('census_division' 'dissemination_area' 'dissemination_block' 'province')
UKLevels=('postcode_area' 'postcode_district' 'postcode_sector' 'postcode_unit')
USLevels=('block' 'block_group' 'county' 'state' 'tract' 'zcta5')

function dump_country(){
  country=$1
  local -n levels=$2
  months=$3

  for level in "${levels[@]}"; do
    psql -U postgres gis --port 5555 --host 0.0.0.0 -Atc "copy (select * from \"$country.mastercard\".mc_$level where month in ($months)) to stdout" > ./mc_${country}_${level}.dump
  done
}

case $country in
    'au' )
        dump_country au AULevels $months
        ;;
    'ca' )
        dump_country ca CALevels $months
        ;;
    'uk' )
        dump_country uk UKLevels $months
        ;;
    'us' )
        dump_country us USLevels $months
        ;;
esac

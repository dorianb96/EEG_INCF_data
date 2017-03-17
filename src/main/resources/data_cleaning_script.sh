#!/usr/bin/env bash

sed -i '.bak' 's/   /;/g' raw_epochs_*.txt; sed -i '.bak' 's/  /;/g' raw_epochs_*.txt; sed -i '.bak' 's/  //g' raw_epochs_*.txt; sed -i '.bak' 's/^.//' raw_epochs_*.txt; sed -i '.bak' 's/  //g' targets.txt

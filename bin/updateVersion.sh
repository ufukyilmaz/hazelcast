find . -name ra.xml | xargs perl -i -pe "s/\<resourceadapter\-version\>.+\<\/resourceadapter\-version\>/<resourceadapter\-version\>$1\<\/resourceadapter\-version\>/"

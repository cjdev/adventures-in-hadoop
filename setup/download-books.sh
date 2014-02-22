#!/bin/bash

bookIdsFile=$1
typeset -l titleString

cat ${bookIdsFile} | while read id title
do
    echo Downloading ${title}
    titleString=$(echo ${title} | sed 's/\s/-/g')
    curl -o ${titleString}.txt http://www.gutenberg.org/files/${id}/${id}.txt
done
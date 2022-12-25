#!/bin/sh
#
# Dump noobaa timelines from all running endpoints
# to standard output as an array of JSON objects
#

namespace="${NOOBAA_NAMESPACE:-noobaa}"

echo "["
for pod in $(kubectl get pods -n $namespace -l noobaa-s3=noobaa -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}')
do
	while IFS= read -r line
	do
		if [ -z "$not_first" ]; then
			not_first=1
        	else 
        		echo ","
		fi
		echo $line
	done <<<"$(kubectl logs -n $namespace $pod | grep Timeline | sed  -E "s/^.*JSON (.*)$/\1/")"
done

echo "]"

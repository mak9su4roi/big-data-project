#! /bin/bash

echo "======================================="
echo "---"

request() {
    echo "url: ${1}"
    printf "response: "
    curl -s -X GET "${1}" | jq . | head
    echo "---"
}

echo "<QueryN: 1>"
request "http://localhost:8080/domains/"

echo "======================================="
echo "---"

echo "<QueryN: 2>"
request "http://localhost:8080/user_id/2092546"

echo "======================================="
echo "---"

echo "<QueryN: 3>"
request "http://localhost:8080/domain-pages/" 

echo "======================================="
echo "---"

echo "<QueryN: 4>"
request "http://localhost:8080/page_id/9431477" 

echo "======================================="